import mysql.connector as mysql
from mysql.connector import Error
import pandas as pd
import boto3, os, schedule, time, sys
from datetime import datetime
from config import CREDENTIALS

source = CREDENTIALS['source']
target = CREDENTIALS['TARGET_DB']

def main():
    # Initializing AWS Credentials to establish the connection with AWS-S3 using Boto3
    s3 = boto3.client('s3', aws_access_key_id=source['AWS_ACCESS_KEY_ID'], aws_secret_access_key=source['AWS_SECRET_ACCESS_KEY']) 

    # get object and file (key) from bucket
    obj = s3.get_object(Bucket= source['BUCKET_NAME'], Key= source['FILE_NAME']) 

    # Accessing the csv file from AWS-S3
    initial_df = pd.read_csv(obj['Body']) # 'Body' is a key word
    initial_df = initial_df.fillna({'MonthlyIncome':0, 'NumberOfDependents':0}, inplace = True)
    initial_df = initial_df.astype(str)

    # print(credentials)
    try:
        conn = mysql.connect(host=target['DB_HOST'], user=target['DB_USER'],  password=target['DB_PASS'])
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE CreditRisk_DB")
            print("testDB database is created")

    except Error as e:
        print("Error while connecting to MySQL", e)

    try:
        conn = mysql.connect(host=target['DB_HOST'], database=target['DB_NAME'], user=target['DB_USER'], password=target['DB_PASS'])
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
            cursor.execute('DROP TABLE IF EXISTS creditrisk;')
            print('Creating table....')
            cursor.execute("""CREATE TABLE creditrisk(
                                id INT(50) PRIMARY KEY, 
                                SeriousDlqin2yrs INT(10) NOT NULL, 
                                RevolvingUtilizationOfUnsecuredLines FLOAT NOT NULL, 
                                age INT(10) NOT NULL,
                                NumberOfTime30to59DaysPastDueNotWorse INT(10) NOT NULL, 
                                DebtRatio FLOAT NOT NULL, 
                                MonthlyIncome INT(10) NOT NULL, 
                                NumberOfOpenCreditLinesAndLoans INT(10) NOT NULL, 
                                NumberOfTimes90DaysLate INT(10) NOT NULL, 
                                NumberRealEstateLoansOrLines INT(10) NOT NULL, 
                                NumberOfTime60to89DaysPastDueNotWorse INT(10) NOT NULL, 
                                NumberOfDependents INT(10) NOT NULL)""")
            print("creditrisk table is created....")
            for i, row in initial_df.iterrows():
                # print(i, tuple(row))
                sql = "INSERT INTO CreditRisk_DB.creditrisk VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                # print("Record inserted")
                # the connection is not autocommitted by default, so we 
                # must commit to save our changes
                conn.commit()

       
    except Error as e:  
        # print("Error while connecting to MySQL", e)
        now = datetime.now()
        current_time = now.strftime("%H:%M:%s")
        data = str("Process Time = " + current_time + " Error while connecting to MySQL")
        if os.path.isfile("log.txt"):
            with open("log.txt", 'a') as f:          
                f.write('\n' + data + str(e))
        else:
            with open("log.txt", "w") as f:
                f.write(data)
        

if __name__ == "__main__":
    # Scheduling the job to update the DB everyday
    schedule.every().day.at("10:30").do(main)
    
    while True:
        schedule.run_pending()
        time.sleep(1)

