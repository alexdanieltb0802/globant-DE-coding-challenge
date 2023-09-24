import csv
import json
import sys
import logging
import rds_config
import pymysql

rds_host  = "globantdb.cnpqon3b6hn0.us-east-1.rds.amazonaws.com"
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name


logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

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
        
        tb_content = map_csv_content(event,list_val)
        insert_toDB(tb_content)
        
        return {
            "statusCode": 200,
            "body": json.dumps('succeed!')
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
    }

def map_csv_content(event,registros):
    map_data={}
    if event['headers']['table'] == 'jobs':
        print('procesing jobs table')
        map_data = {
            "jobs": [
                {
                    "id": int(val[0]),
                    "job": val[1]
                }
                for val in registros
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
                for val in registros
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
                    "department_id": 'NULL' if val[3]=='' else int(val[3]),
                    "job_id": 'NULL' if val[4]=='' else int(val[4])
                }
                for val in registros
            ]
        }
        
        print(map_data)
    return map_data

def insert_toDB(tb_content):
    #inserting data from departments:
    if 'departments' in tb_content:
        query_departments = "INSERT INTO departments(id,department) VALUES "
        for deparment in tb_content['departments']:
            query_departments+=f"({deparment['id']},'{deparment['department']}'),"
        query_departments = query_departments[:-1]
        with conn.cursor() as cur:
            cur.execute(query_departments)
        conn.commit()
    ##Insertamos data de jobs
    if 'jobs' in tb_content:
        query_jobs = "INSERT INTO jobs(id,job) VALUES "
        for job in tb_content['jobs']:
            query_jobs+=f"({job['id']},'{job['job']}'),"
        query_jobs = query_jobs[:-1]
        with conn.cursor() as cur:
            cur.execute(query_jobs)
        conn.commit()
    ##Insertamos data de hired_employees
    if 'hired_employees' in tb_content:
        query_employee = "INSERT INTO hired_employees(id,name,datetime,department_id,job_id) VALUES "
        for employee in tb_content['hired_employees']:
            query_employee+=f"({employee['id']},'{employee['name']}','{employee['datetime']}',{employee['department_id']},{employee['job_id']}),"
        query_employee = query_employee[:-1]
        with conn.cursor() as cur:
            cur.execute(query_employee)
        conn.commit()