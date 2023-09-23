import json

def lambda_handler(event, context):
    try:
        # Extract the CSV data from the event
        csv_data = event["body"]
        
        # Parse the CSV data into a list of dictionaries
        csv_list = []
        csv_reader = csv.DictReader(csv_data.splitlines())
        for row in csv_reader:
            csv_list.append(row)
        
        # You now have a list of dictionaries representing the CSV data
        # You can perform any required transformations or processing here
        
        # For demonstration purposes, return the list of dictionaries as JSON
        return {
            "statusCode": 200,
            "body": json.dumps(csv_list)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
    }
