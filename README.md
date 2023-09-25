# globant-DE-coding-challenge

This repository is focused on solving the proposed challenges and storing the required extraction, transformation, and loading files. It also contains the complementary files for this purpose.

The main structure is as follows:
1. Lambda_function:
  - function in charge of hosting the REST service for frequent data loading.
  - Some considerations for requesting the API are as follows:
    - Send data this url: https://8w6e72tw6k.execute-api.us-east-1.amazonaws.com/frecuent-load-lambda
    - Consider **POST method**
    - Edit **"Content-Type" HEADER** value **"text/csv"** 
    - Edit the value of the **"table" HEADER** according to the corresponding **CSV file** that is required to be sent. For example, if you want to load the jobs.csv file, the value of the "table" header must be "jobs".  

2. Sql
  - Database generation script
    - For database implementation in AWS RDS, it's necessary to open internet access modifying public access from the security group of VPC
  - View 1: challenge 2, requirement 1
    -  Shows number of employees hired per quarter in a given job
  - View 2: challenge 2, requirement 2
    - Counts the number of employees per department hired in 2021 greater than the average number of employees hired in 2021 for all departments, viewed in descending order

3. res: Folder for all static resources in the project. For example, dashboards, images, etc.
  - Aws diagram architecture

4. AWS architecture of the challenge

![Alt Text](res/aws_diagram.jpeg?raw=true "AWS architecture")