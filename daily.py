import json
from utils import AVERAGE_DEMAND_HRS
from datetime import date, timedelta
import random
import boto3
# import datetime
from datetime import datetime, timedelta
client = boto3.client('timestream-query')

daily_table_name = "daily-data-table"
hourly_table_name = "hourly-data-table"
dynamodb = boto3.resource('dynamodb')

# Instantiate a daily table and hourly table
daily_table = dynamodb.Table(daily_table_name)
hourly_table = dynamodb.Table(hourly_table_name)

def get_station_run_outs(item, start_day, end_day, for_day = 'multi'):
    """
    function returns count of station runouts. 
    Parameters : 
        start_day - start date with hours and minutes
        end_day -   end date with hours and minutes
        for_day - one / multi ( shows more than one day or just one day)
    """
    
    if for_day == 'one':
        # these condition us used for single day selection.
        station_runout_status = {}
        
        # here the query measure station run downtime and returns the count of how many times did station run out
        response = client.query(
            QueryString="""
                    SELECT count(*) as station_runout FROM "iwatani_nel_timestreamdatatable_db"."iwatani_nel_timestream_telemetry_table" 
                    where SerialNumber = '{}' and time between '{}' and date_add('hour', 1, TIMESTAMP '{}') 
                    and PT_Storage2 < 100 and PT_Storage3 < 100 and PT_Storage4 < 100 and Station_Available = 0.0
            """.format(item, start_day, start_day)
        )

        
        for ind, res_val in enumerate(response['Rows'][0]['Data']):
            station_runout_status[response['ColumnInfo'][ind]['Name']] = int(res_val['ScalarValue'])
        
        return station_runout_status['station_runout']
    else:
        # these condition is for more than one day
        station_runout_status = {}
        
        # here the query measure station run downtime and returns the count of how many times did station run out
        response = client.query(
            QueryString="""
                    SELECT count(*) as station_runout FROM "iwatani_nel_timestreamdatatable_db"."iwatani_nel_timestream_telemetry_table" 
                    where SerialNumber = '{}' and time between '{} 00:00:00' and '{} 24:00:00' 
                    and PT_Storage2 < 100 and PT_Storage3 < 100 and PT_Storage4 < 100 and Station_Available = 0.0
            """.format(item, start_day, end_day)
        )
        #print(response) 
        
        for ind, res_val in enumerate(response['Rows'][0]['Data']):
            station_runout_status[response['ColumnInfo'][ind]['Name']] = int(res_val['ScalarValue'])
        
        return station_runout_status['station_runout']

def get_avg_mass_delivered(item, start_date, end_date, for_day = 'multi'):
    """
    function returns average deliverd mass. 
    Parameters : 
        start_day - start date with hours and minutes
        end_day -   end date with hours and minutes
        for_day - one / multi ( shows more than one day or just one day)
    """
    
    if for_day == 'one':
        # these condition us used for single day selection.
        
        start_day, start_time = start_date.split(' ')
        start_time = start_time.split('.')[0]
        
        # reading from hourly DDB, and summing the average. 
        response = hourly_table.get_item(
                Key={
                    'SerialNumber': item,
                    'day_date': start_day,
                    'day_time': start_time
                }
            )
        avg_delivered_mass = 0  
        if 'Item' in response.keys():
            if 'avg_delivered_mass' in response["Item"].keys():
                avg_delivered_mass = float(response["Item"]['avg_delivered_mass'])
            else:
                avg_delivered_mass = 0

        return avg_delivered_mass  
        
    else:
        # these condition us used for Multi days selection.
        start_year, start_month, start_day = start_date.split('-')
        end_year, end_month, end_day =  end_date.split('-') 
        
        delta = datetime(int(end_year), int(end_month), int(end_day)) -  datetime(int(start_year), int(start_month), int(start_day))
        #print (delta.days)
        if delta.days > 1:
            custom_date_list = date_range(datetime(int(start_year), int(start_month), int(start_day)), datetime(int(end_year), int(end_month), int(end_day)))
            #print('Custome list : ', custom_date_list)
            
        else:
            custom_date_list = [start_day]
            #print('Custome list : ', custom_date_list)
            
        delivered_data = []
        
        # here the query measure station run downtime and returns the count of how many times did station run out
        
        month_list = []
        avg_mass_year = []
        #print('Custom list : ', custom_date_list)
        for date_day in custom_date_list:
            # reading from daily DDB
            response = daily_table.get_item(
                Key={
                    'SerialNumber': str(item), 
                    'day_date': date_day ### This is line giving issue. Have added SerialNumber. Need to change day_date to date_time . Please check
                }
            )
            
            if 'Item' in response.keys():
                if 'avg_delivered_mass' in response["Item"].keys():
                    avg_mass_year.append(float(response["Item"]['avg_delivered_mass']))
                else:
                    avg_mass_year.append(0)
            
            #print(avg_mass_year)
            
        
        #print('Month list : ',month_list)
        if len(avg_mass_year) == 0:
            return 0
        else:
            return round(sum(avg_mass_year) / len(avg_mass_year), 2)
    
def get_delivery_count(item, start_day, end_day,for_day = 'multi'):
    """
    function returns average deliverd count. 
    Parameters : 
        start_day - start date with hours and minutes
        end_day -   end date with hours and minutes
        for_day - one / multi ( shows more than one day or just one day)
    """
    
    if for_day == 'one':
        # these condition us used for single day selection.
        
        avg_delivery_status = {}
        
            
        """
        here the query to get the count of delivery using lead and over windows function
        https://docs.aws.amazon.com/timestream/latest/developerguide/window-functions.html
        https://www.geeksforgeeks.org/sql-server-lead-function-overview/
        """
        response = client.query(
            QueryString="""
                        with mass_table as (
                        SELECT time as tf, mass_PT_Storage4+mass_PT_Storage3+mass_PT_Storage2+mass_PT_Storage1 as total_tube_mass, 
                        lead(mass_PT_Storage4+mass_PT_Storage3+mass_PT_Storage2+mass_PT_Storage1) over(order by time) as next_total_tube_mass 
                        FROM "iwatani_nel_timestreamdatatable_db"."iwatani_nel_timestream_telemetry_table" 
                        WHERE SerialNumber = '{}' and time between '{}' and date_add('hour', 1, TIMESTAMP '{}')
                        )
                        
                        select count(*) as total, count_if(total_tube_mass < next_total_tube_mass) as delivery_count from mass_table
            """.format(item, start_day, start_day)
        )

        for ind, res_val in enumerate(response['Rows']):
            #print('Res val : ',res_val)
            for index, val in enumerate(res_val['Data']):
                #print('Val : ',res_val)
                avg_delivery_status[response['ColumnInfo'][index]['Name']] = int(val['ScalarValue'])
        
        return avg_delivery_status['delivery_count']
    else:
       
        avg_delivery_status = {}
    
        """
        here the query to get the count of delivery using lead and over windows function
        https://docs.aws.amazon.com/timestream/latest/developerguide/window-functions.html
        https://www.geeksforgeeks.org/sql-server-lead-function-overview/
        """
        response = client.query(
            QueryString="""
                        with mass_table as (
                        SELECT time as tf, mass_PT_Storage4+mass_PT_Storage3+mass_PT_Storage2+mass_PT_Storage1 as total_tube_mass, 
                        lead(mass_PT_Storage4+mass_PT_Storage3+mass_PT_Storage2+mass_PT_Storage1) over(order by time) as next_total_tube_mass 
                        FROM "iwatani_nel_timestreamdatatable_db"."iwatani_nel_timestream_telemetry_table" WHERE SerialNumber = '{}' and time between '{} 00:00:00' and '{} 24:00:00'
                        )
                        
                        select count(*) as total, count_if(total_tube_mass < next_total_tube_mass) as delivery_count from mass_table
            """.format(item, start_day, end_day)
        )
        
        for ind, res_val in enumerate(response['Rows']):
            for index, val in enumerate(res_val['Data']):
                avg_delivery_status[response['ColumnInfo'][index]['Name']] = int(val['ScalarValue'])
        
        return avg_delivery_status['delivery_count']

def get_avg_demand(item, start_day, end_day, for_day = 'multi'):
    """
    These function returns and calculates sum of refuel mass from refuel timestream DB between requested duration
    Parameters : 
        start_day - start date with hours and minutes
        end_day -   end date with hours and minutes
        for_day - one / multi ( shows more than one day or just one day)
    """
    if for_day == 'one':
        # these condition us used for single day selection.
        
        avg_demand_status = {}
        
        # here the query measure station run downtime and returns the count of how many times did station run out
        response = client.query(
            QueryString="""
                        SELECT sum(Refuel_Mass) as refuel_mass FROM "iwatani_nel_timestreamdatatable_db"."iwatani_nel_timestream_refuel_table" 
                        WHERE SerialNumber = '{}' and time between '{}' and date_add('hour', 1, TIMESTAMP '{}')
            """.format(item, start_day, start_day)
        )
    
        
        for ind, res_val in enumerate(response['Rows']):
    
            for index, val in enumerate(res_val['Data']):
                if 'ScalarValue' in val:
                    avg_demand_status[response['ColumnInfo'][index]['Name']] = float(val['ScalarValue'])
                else:
                    avg_demand_status['refuel_mass'] = 0
        return avg_demand_status['refuel_mass']
        
    else:
        avg_demand_status = {}
        
        # here the query measure station run downtime and returns the count of how many times did station run out
        response = client.query(
            QueryString="""
                        SELECT sum(Refuel_Mass) as refuel_mass FROM "iwatani_nel_timestreamdatatable_db"."iwatani_nel_timestream_refuel_table" 
                        WHERE SerialNumber = '{}' and time between '{} 00:00:00' and '{} 24:00:00'
            """.format(item, start_day, end_day)
        )
    
        
        for ind, res_val in enumerate(response['Rows']):
    
            for index, val in enumerate(res_val['Data']):
                if 'ScalarValue' in val:
                    avg_demand_status[response['ColumnInfo'][index]['Name']] = float(val['ScalarValue'])
                else:
                    avg_demand_status['refuel_mass'] = 0
        return avg_demand_status['refuel_mass']

def get_avg_inventory(serial, start_day, end_day, for_day = 'multi'):

    """
    These function returns and calculates sum of tube mass  from telemetry timestream DB between requested duration
    Parameters : 
        start_day - start date with hours and minutes
        end_day -   end date with hours and minutes
        for_day - one / multi ( shows more than one day or just one day)
    """
    if for_day == 'one':
        # these condition us used for single day selection.
        
        avg_inventory_status = {}
        
        # here the query measure avg of pt_storage
        response = client.query(
            QueryString="""
                        with mass_table as (
                        SELECT time as tf, PT_Storage4+PT_Storage3+PT_Storage2+PT_Storage1 as total_tube_mass
                        FROM "iwatani_nel_timestreamdatatable_db"."iwatani_nel_timestream_telemetry_table"
                        WHERE SerialNumber = '{}' and time between '{}' and date_add('hour', 1, TIMESTAMP '{}')
                        )
                        
                        select avg(total_tube_mass) as inventory_mass from mass_table
            """.format(item, start_day, start_day)
        )
    
        
        for ind, res_val in enumerate(response['Rows']):
    
            for index, val in enumerate(res_val['Data']):
                if 'ScalarValue' in val:
                    avg_inventory_status[response['ColumnInfo'][index]['Name']] = float(val['ScalarValue'])
                else:
                    avg_inventory_status['inventory_mass'] = 0
        return round(avg_inventory_status['inventory_mass'], 2)
    
    else:
        # these condition us used for multi days selection.      
        avg_inventory_status = {}
        
        #  here the query measure avg/sum of pt_storage
        response = client.query(
            QueryString="""
                        with mass_table as (
                        SELECT time as tf, PT_Storage4+PT_Storage3+PT_Storage2+PT_Storage1 as total_tube_mass
                        FROM "iwatani_nel_timestreamdatatable_db"."iwatani_nel_timestream_telemetry_table"
                        WHERE SerialNumber = '{}' and time between '{} 00:00:00' and '{} 24:00:00'
                        )
                        
                        select avg(total_tube_mass) as inventory_mass from mass_table
            """.format(item, start_day, end_day)
        )
    
        
        for ind, res_val in enumerate(response['Rows']):
    
            for index, val in enumerate(res_val['Data']):
                if 'ScalarValue' in val:
                    avg_inventory_status[response['ColumnInfo'][index]['Name']] = float(val['ScalarValue'])
                else:
                    avg_inventory_status['inventory_mass'] = 0
        return round(avg_inventory_status['inventory_mass'], 2)
    
def get_transfer_time(item, start_date, end_date, for_day = 'multi'):
    
    """
    These function returns avg transfer time from daily and hourly DDB 
    Parameters : 
        start_day - start date with hours and minutes
        end_day -   end date with hours and minutes
        for_day - one / multi ( shows more than one day or just one day)
    """
    
    if for_day == 'one':
        # these condition us used for single day selection.
        
        start_day, start_time = start_date.split(' ')
        start_time = start_time.split('.')[0]
        
        response = hourly_table.get_item(
                Key={
                    'SerialNumber': item,
                    'day_date': start_day,
                    'day_time': start_time
                    
                }
            )
        avg_transfer_time = 0  
        if 'Item' in response.keys():
                if 'avg_transfer_time' in response["Item"].keys():
                    avg_transfer_time = float(response["Item"]['avg_transfer_time'])
                else:
                    avg_transfer_time = 0

        return avg_transfer_time   
    else:
        # these condition us used for multi days selection.
        start_year, start_month, start_day = start_date.split('-')
        end_year, end_month, end_day =  end_date.split('-') 
        
        delta = datetime(int(end_year), int(end_month), int(end_day)) -  datetime(int(start_year), int(start_month), int(start_day))
        
        # creating a list for day iteration 
        if delta.days > 1:
            custom_date_list = date_range(datetime(int(start_year), int(start_month), int(start_day)), datetime(int(end_year), int(end_month), int(end_day)))
        
        else:
            custom_date_list = [start_day]
          
        avg_transfer_time_year = []
        for month in custom_date_list:
            response = daily_table.get_item(
                Key={
                    'day_date': month
                }
            )
            
            if 'Item' in response.keys():
                if 'avg_transfer_time' in response["Item"].keys():
                    avg_transfer_time_year.append(float(response["Item"]['avg_transfer_time']))
                else:
                    avg_transfer_time_year.append(0)
        if len(avg_transfer_time_year) == 0:
            return 0
        else:
            return round(sum(avg_transfer_time_year) / len(avg_transfer_time_year), 2)


def date_range(start, end) -> list:
    #these function return list of days/month/week in list based on delta. 
    delta = end - start
    
    days = []
    for i in range(delta.days + 1):
        day = start + timedelta(days=i) 
        cur_mon = str(day.month)
        cur_day = str(day.day)
        if len(cur_mon) == 1:
            cur_mon = '0'+str(day.month)
        if len(cur_day) == 1:
            cur_day = '0'+str(day.day)
        day_date = str(day.year)+'-'+cur_mon+'-'+cur_day
        days.append(day_date)
        
    return days 

def get_date(event : dict, serial) -> dict:
    #these function return the result for report based on day/week/month
    
    result = { }

    start_date_list = event["start_date"].split('-')
    end_date_list = event["end_date"].split('-')
    
    start_date = date(int(start_date_list[0]), int(start_date_list[1]), int(start_date_list[-1])) 
    end_date = date(int(end_date_list[0]), int(end_date_list[1]), int(end_date_list[-1]))    # perhaps date.now()
    delta = end_date - start_date   # returns timedelta
    
    # here the report is generates in weeks
    if delta.days >= 31 and delta.days < 180:
        result = convert_to_week(start_date, end_date)
        return result
        
    #here the report is generated in months
    elif delta.days >= 180:
        result = convert_to_months(start_date, end_date)
        return result
        
    #here the report is generated in days.
    elif delta.days > 1 and delta.days<31:
        for i in range(delta.days + 1):
            
            day = start_date + timedelta(days=i)
            print('day : ', day)
            print(event["start_date"], str(day))
            result[str(day)] = {
            "average_demand": get_avg_demand(serial,str(day),str(day)),
            "average_delivery_volume": get_avg_mass_delivered(serial,str(day),str(day)),
            "delivery_count" : get_delivery_count(serial,str(day),str(day)),
            "average_inventory": get_avg_inventory(serial,str(day),str(day)),
            "average_transfer_time" : get_transfer_time(serial,str(day),str(day)),
            "station_runout": get_station_run_outs(serial,str(day),str(day))
            }
    #here the report is generated in hours.
    else:
        for key, val in AVERAGE_DEMAND_HRS.items():
            result[key] = {
                "average_demand": get_avg_demand(serial, event["start_date"]+' '+key, event["end_date"], for_day = 'one'),
                "average_delivery_volume": get_avg_mass_delivered(serial, event["start_date"]+' '+key, event["end_date"], for_day = 'one'),
                "delivery_count" : get_delivery_count(serial, event["start_date"]+' '+key, event["end_date"], for_day = 'one'),
                "average_inventory": get_avg_inventory(serial, event["start_date"]+' '+key, event["end_date"], for_day = 'one'),
                "average_transfer_time" : get_transfer_time(serial, event["start_date"]+' '+key, event["end_date"], for_day = 'one'),
                "station_runout": get_station_run_outs(serial, event["start_date"]+' '+key, event["end_date"], for_day = 'one'),
                }
        
    return result

def convert_to_months(start_date, end_date):
    """
    here the report is generated for months
    Parameters : 
        start_day - start date with hours and minutes
        end_day -   end date with hours and minutes
    """
    delta = end_date - start_date
    total_days = delta.days
    result = { }
    month_list = [29]*(total_days // 30)
    
    month_start_day = start_date
    
    if total_days % 30 != 0:
        month_list.append(total_days % 30)

    for days in month_list:
        month_end_day = month_start_day + timedelta(days=days)
        result[str(month_start_day)+' '+str(month_end_day)] = {
        "average_demand": get_avg_demand(str(month_start_day), str(month_end_day)),
        "average_delivery_volume": get_avg_mass_delivered(str(month_start_day), str(month_end_day)),
        "delivery_count" : get_delivery_count(str(month_start_day), str(month_end_day)),
        "average_inventory": get_avg_inventory(str(month_start_day), str(month_end_day)),
        "average_transfer_time" : get_transfer_time(str(month_start_day), str(month_end_day)),
        "station_runout": get_station_run_outs(str(month_start_day), str(month_end_day))
        }
        month_start_day = month_end_day+timedelta(days=1)
        
    
    return result
    
def convert_to_week(start_date, end_date):
    """
    function returns report in weeks
    Parameters : 
        start_day - start date with hours and minutes
        end_day -   end date with hours and minutes
    """
    delta = end_date - start_date
    total_days = delta.days
    result = { }
    week_list = [6]*(total_days // 7)
    
    week_start_day = start_date
    
    if total_days % 7 != 0:
        week_list.append(total_days % 7)
        
    for days in week_list:
        week_end_day = week_start_day + timedelta(days=days)
        
        result[str(week_start_day)+' '+str(week_end_day)] =  {
        "average_demand": get_avg_demand(str(week_start_day), str(week_end_day)),
        "average_delivery_volume": get_avg_mass_delivered(str(week_start_day), str(week_end_day)),
        "delivery_count" : get_delivery_count(str(week_start_day), str(week_end_day)),
        "average_inventory": get_avg_inventory(str(week_start_day), str(week_end_day)),
        "average_transfer_time" : get_transfer_time(str(week_start_day), str(week_end_day)),
        "station_runout": get_station_run_outs(str(week_start_day), str(week_end_day))
        }
        week_start_day = week_end_day+timedelta(days=1)
        
    return result

def lambda_handler(event, context):
    
    # handler function returns for xls/csv report for dispenser page for Distribution page.

    response = {}
    
    ssm_client = boto3.client('ssm')
    
    try:
        station_name = event['queryStringParameters']['station']
    except:
        station_name = 'Hawaiian Gardens'
    
    station_data = ssm_client.get_parameter(Name='station_data')
    station_values = json.loads(station_data['Parameter']['Value'])[station_name]

    for serial in station_values:
        if event:
            if type(event) == str:
                
                response['station'] = get_date(json.loads(event), serial)
            else:
                response['station'] = get_date(event, serial)
        else:  
            event = {
                    "start_date" : "2022-11-7",
                    "end_date": "2022-11-13"
                }
            response[str(item)] = get_date(event, serial)
        
        
    return {
        'statusCode': 200,
        'body': response
    }
