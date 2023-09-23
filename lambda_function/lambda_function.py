import csv
import json

def lambda_handler(event, context):
    try:
        # Extract the CSV data from the event
        csv_data = event["body"]
        # Parse the CSV data into a list of dictionaries
        csv_list = []
        csv_reader = csv.reader(csv_data.splitlines())
        for row in csv_reader:
            csv_list.append(row)
        
        
        print('reading csv file')
        print(csv_list)
        list_val = csv_list
        
        if event['headers']['table'] == 'jobs':
            print('procesing jobs table')
            map_data = {
                "jobs": [
                    {
                        "id": int(val[0]),
                        "job": val[1]
                    }
                    for val in list_val
                ]
            }
            
            print(map_data)
        
        if event['headers']['table'] == 'departments':
            print('procesing departments table')
            map_data = {
                "departments": [
                    {
                        "id": int(val[0]),
                        "department": val[1]
                    }
                    for val in list_val
                ]
            }
            
            print(map_data)
        
        if event['headers']['table'] == 'hired_employees':
            print('procesing hired_employees table')
            map_data = {
                "hired_employees": [
                    {
                        "id": int(val[0]), 
                        "name": val[1],
                        "datetime": val[2],
                        "department_id": None if val[3]=='' else int(val[3]),
                        "job_id": None if val[4]=='' else int(val[4])
                    }
                    for val in list_val
                ]
            }
            
            print(map_data)
        
        return {
            "statusCode": 200,
            "body": json.dumps(map_data)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
    }