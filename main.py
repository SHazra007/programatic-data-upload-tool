import boto3
import csv
import os
import time
from datetime import datetime
from io import StringIO



from config import BUCKET, companies

start_time = time.time()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.environ.get("ACCESS_KEY"),
    aws_secret_access_key=os.environ.get("SECRET_KEY"),
)


def get_records_from_s3(company):
    today = datetime.today().strftime("%m_%d_%Y")
    key = f'{company}/{today}/raw_employees_data.csv'
    response = s3.get_object(
        Bucket=BUCKET,
        Key=key,
    )
    data = response['Body'].read().decode('utf-8')
    return data


def parse_data_from_records(records):
    reader = csv.DictReader(StringIO(records))
    return [row for row in reader]


if __name__ == '__main__':
    employees = []
    for company in companies:
        records = get_records_from_s3(company)
        records = parse_data_from_records(records)
        employees.extend(records)
    print(len(employees))


#     #
#     if company == "Infosys":
#         key = f"{company}/{today}/Employee_Salaries_-_2020.csv"
#     else:
#         key = f"{company}/{today}/Employee_Salaries_-_2022.csv"
#
#     print(key)
#     s1 = time.time()
#     response = s3.head_object(
#         Bucket=BUCKET,
#         Key=key,
#     )
#     # data = response['Body'].read().decode('utf-8')
#     print(response["ETag"], time.time() - s1)
#     # reader = csv.DictReader(StringIO(data))
#     # for row in reader:
#     #     l.append(row)
# print(len(l))
# end_time = time.time()
# print("time difference: ", end_time - start_time)
#
# # print(l)
#
#
# # import pandas as pd
# #
# # for company in companies:
# #     # key = f'{company}/{today}/raw_employees_data.csv'
# #     if company == 'Infosys':
# #         key = f'{company}/{today}/Employee_Salaries_-_2020.csv'
# #     else:
# #         key = f'{company}/{today}/Employee_Salaries_-_2022.csv'
# #
# #     print(key)
# #     response = s3.get_object(
# #             Bucket=BUCKET,
# #             Key=key,
# #         )
# #     data = response['Body']
# #     print(response['ETag'])
# #     reader = pd.read_csv(data)
# #     for row in reader:
# #         l.append(row)
# # print(len(l))
# # end_time = time.time()
# # print("time difference: ", end_time - start_time)
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# #
# # #Trying the integration
# #
# # # from fastapi import FastAPI
# # # import boto3
# # #
# # # app = FastAPI()
# # # s3 = boto3.client('s3')
# #
# # # Initialize data (you can replace this with your own data loading logic)
# # # csv_records = []
# # #
# # # @app.get("/data")
# # # async def get_data():
# # #     return {"data": data}
# #
# # # Periodically update the data from S3 (you should adjust the interval)
# # # import asyncio
# #
# # # async def update_data_from_s3():
# # #     data = response['Body'].read().decode('utf-8')
# # #     reader = csv.DictReader(StringIO(data))
# # #     csv_records = [row for row in reader]
# # #     while True:
# # #         new_data = reader()  # Replace with your S3 data loading logic
# # #         if new_data != csv_records:
# # #             data.clear()
# # #             csv_records.extend(new_data)
# # #         await asyncio.sleep(3600)  # Check every hour
# # #         return csv_records
# # #
# # # if __name__ == "__main__":
# # #     loop = asyncio.get_event_loop()
# # #     csv_red = loop.create_task(update_data_from_s3())
# #
# #     # import uvicorn
# #     # uvicorn.run(app, host="0.0.0.0", port=8000)
# #
# #
