from django.shortcuts import render
from django.http import HttpResponse
from openpyxl import Workbook
import csv
import requests
import gzip
import io
from SmartApi import SmartConnect
import pyotp
from logzero import logger
from websocket import create_connection
import json
from io import StringIO
import datetime
import time 
import websocket
from datetime import datetime, timedelta
from redis import RedisCluster
import boto3
import os 
from django.core.cache import cache
from django.core.cache import cache
from django.http import JsonResponse
from django.core.mail import EmailMessage
from openpyxl import Workbook
from celery import shared_task
import pickle
import base64


import pandas as pd
# session = boto3.Session(
#     aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
#     aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
#     aws_session_token=os.environ["AWS_SESSION_TOKEN"],
# )



# # Test it on a service (yours may be different)
# s3 = boto3.client('s3')
# sts_client = boto3.client('sts')
# assumed_role_object = sts_client.assume_role(
#     RoleArn="arn:aws:iam::732165046977:role/ecs-task-role",
#     RoleSessionName="AssumeRoleSession1"

# )


# credentials = assumed_role_object['Credentials']
# print(credentials)
# Create a session with the temporary credentials
# session = boto3.Session(
#     aws_access_key_id=credentials['AccessKeyId'],
#     aws_secret_access_key=credentials['SecretAccessKey'],
#     aws_session_token=credentials['SessionToken']
# )

# Create an S3 client
# s3 = session.client('s3')
s3 = boto3.client('s3')

def get_latest_token_master():
    date = datetime.now() - timedelta(days=0)
    BUCKET = 'angel-server-data-production'
    PREFIX = 'chart-data-backup/nse_equity/master_token'
    while True:
        key = f'{PREFIX}/{date.strftime("%Y-%m-%d")}/nse_equity_isin_token.csv'
        print(key)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            df = pd.read_csv(obj['Body'])
            print(f"Found file for date: {date.strftime('%Y-%m-%d')}")
            print(df.head(20))
            return df
        except s3.exceptions.NoSuchKey:
            print(f"File not found for date: {date.strftime('%Y-%m-%d')}, trying previous day.")
            date -= timedelta(days=1)
        except Exception as e:
            print(f"An error occurred: {e}")
            # date -= timedelta(days=1)

# s3 = boto3.client('s3')
# obj = s3.get_object(Bucket='angel-server-data-production', Key='/nse_equity/master_token/2024-06-07/nse_equity_tokenmaster.csv')
df = get_latest_token_master()
print(df.head(20))

tokens_list = []
for index, row in df.iterrows():
    tokens_list.append(str(row['token']))

# print(tokens_list)
# print(len(tokens_list))

def parse_falcon_data(falcon_data):
    # Initialize an empty list to store the parsed data
    parsed_data = []

    # Iterate through each line in the falcon_data StringIO object
    for line in falcon_data:
        # Split the line by commas to separate the values
        values = line.strip().split(',')

        # Extract the relevant values and convert them to the appropriate data types
        date_time = values[0]
        open_price = float(values[1])
        high_price = float(values[2])
        low_price = float(values[3])
        close_price = float(values[4])
        volume = int(values[5])

        # Append the extracted values to the parsed_data list as a sublist
        parsed_data.append([date_time, open_price, high_price, low_price, close_price, volume])

    return parsed_data


def get_interval_constant(interval, api):
    if api == "Angel Broking":
        if interval == "1minute":
            return "ONE_MINUTE"
        elif interval == "30minute":
            return "THIRTY_MINUTE"
        elif interval == "day":
            return "ONE_DAY"
        elif interval == "week":
            return "ONE_DAY"  # Adjust according to Angel Broking API's supported intervals
        elif interval == "month":
            return "ONE_DAY"  # Adjust according to Angel Broking API's supported intervals
    elif api == "Upstox":
        return interval  # Upstox API uses the same intervals as user input
    elif api == "Falcon":
        if interval == "1minute":
            return "1"
        elif interval == "30minute":
            return "30"
        elif interval == "day":
            return "0"
def generate_excel(request, stock_name, from_date, to_date, interval, comparison_type):
    print("inside" + "generate_excel")
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    response = requests.get(url)

    # Checking if request was successful
    if response.status_code != 200:
        return HttpResponse("Failed to fetch data from Upstox API.")

    # Decompressing and reading the CSV data
    with gzip.open(io.BytesIO(response.content)) as f:
        reader = csv.reader(io.TextIOWrapper(f, encoding='utf-8'))
        next(reader)  # Skipping the header row
        data = [row for row in reader]

    # Filtering NSE_EQ entries and extracting ISIN
    def filter_nse_eq_and_get_isin(data, stock_name):
        nse_eq_entries = [row for row in data if row[2] == stock_name]
        for entry in nse_eq_entries:
            parts = entry[0].split('|')
            if len(parts) == 2:
                return parts[1], entry[1]  # Return ISIN and exchange_token
        return None, None

    # Get ISIN and exchange_token
    isin, exchange_token = filter_nse_eq_and_get_isin(data, stock_name)
    print("isin, exchangetoken", isin, exchange_token)
    # Check if ISIN is fetched successfully
    if isin is None:
        return HttpResponse("Stock not found or not available for comparison.")

    # Angel Broking credentials
    api_key = 'JDldPrre'
    username = 'A912156'
    pwd = '3628'

    # Angel Broking
    smartApi = SmartConnect(api_key)
    try:
        totp = pyotp.TOTP("Z5VGZGBBMT6SWOLCESOEDRLJHA").now()
    except Exception as e:
        logger.error("Invalid Token: The provided token is not valid.")
        raise e

    data = smartApi.generateSession(username, pwd, totp)

    if data['status'] == False:
        logger.error(data)
        return HttpResponse("Failed to authenticate with Angel Broking API.")
    else:
        authToken = data['data']['jwtToken']
        refreshToken = data['data']['refreshToken']
        feedToken = smartApi.getfeedToken()
        res = smartApi.getProfile(refreshToken)
        smartApi.generateToken(refreshToken)
        res = res['data']['exchanges']

    # Fetching data from APIs
    try:
        # Angel Broking API Call
        from_date_angel = from_date + " 00:00"  # Add 09:15 AM as start time for 1 minute or 30 minutes interval
        to_date_angel = to_date + " 15:29"  # Add 03:29 PM as end time for 1 minute or 30 minutes interval

        print("got interval ", interval)
        angel_broking_interval = get_interval_constant(interval, "Angel Broking")
        print("angel interval is ", angel_broking_interval)
        historicParam = {
            "exchange": "NSE",
            "symboltoken": exchange_token,
            "interval": angel_broking_interval,
            "fromdate": from_date_angel,
            "todate": to_date_angel
        }
        angel_broking_data = smartApi.getCandleData(historicParam)
        print(historicParam)
        print("Angel Broking data:", angel_broking_data)
        final_data = ""

        # Upstox/Falcon API Call
        if comparison_type == "Upstox":
            upstox_interval = get_interval_constant(interval, "Upstox")
            url = f'https://api.upstox.com/v2/historical-candle/NSE_EQ%7C{isin}/{upstox_interval}/{to_date}/{from_date}'
            headers = {
                'Accept': 'application/json'
            }
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                upstox_data = response.json()['data']['candles']
                upstox_data.reverse()
                final_data = upstox_data
                print("upstox data", upstox_data)
            else:
                logger.error(f"Error: {response.status_code} - {response.text}")
                return HttpResponse(f"Error: {response.status_code} - {response.text}")

        if comparison_type == "Falcon":
            falcon_interval = get_interval_constant(interval, "Falcon")
            from_date_falcon = from_date + "T03:46:00.0"  # Add 09:15 AM as start time for 1 minute or 30 minutes interval
            to_date_falcon = to_date + "T12:01:00.0"  # Add 03:29 PM as end time for 1 minute or 30 minutes interval
            print(to_date)
            period = "I"

            if falcon_interval == "0":
                period = "D"

                to_date_obj = datetime.datetime.strptime(to_date, "%Y-%m-%d")
                new_date_obj = to_date_obj + datetime.timedelta(days=0)
                new_date_str = new_date_obj.strftime("%Y-%m-%d")

                from_date_obj = datetime.datetime.strptime(from_date, "%Y-%m-%d")
                from_new_date_obj = from_date_obj + datetime.timedelta(days=0)
                from_new_date_str = from_new_date_obj.strftime("%Y-%m-%d")

                from_date_falcon = from_new_date_str + "T00:00:00.0"
                to_date_falcon = new_date_str + "T00:00:00.0"  # Add 03:29 PM as end time for 1 minute or 30 minutes interval

            print(from_date_falcon)
            print(to_date_falcon)
            print(exchange_token)
            print(period)

            # ws = create_connection("wss://falconchart.angelbroking.com/")
            # if not ws.connected:
            #     raise Exception("WebSocket connection failed to establish")


            # print("here")
            # try:
            #     body = json.dumps(
            #         {'SeqNo': '1', "Topic": "17." + str(exchange_token), "RType": "OHLCVRANGE", "Action": "req",
            #          "Body": json.dumps({"Period": period, "Minutes": falcon_interval, "FromDate": from_date_falcon,
            #                              "ToDate": to_date_falcon})})
            #     ws.send(body);
            #     print("falcon request body ", body)
            # except Exception as e:
            #     print("exception in falcon request : ", e)

            # resp = json.loads(ws.recv())
            # print("response received",resp)
            # data = resp.get('Body')
            # falcon_data = StringIO(data)

            # final_data = parse_falcon_data(falcon_data)
            # print("************************************************************")
            # print(final_data)
            # print("************************************************************")

            max_retries = 5
            retry_delay = 2  # seconds
            retries = 0
            falcon_data = None

            while retries < max_retries:
                try:
                    ws = create_connection("wss://falconchart.angelbroking.com/")
                    ws.settimeout(4)  # Set a timeout of 10 seconds (adjust as needed)

                    if not ws.connected:
                        raise Exception("WebSocket connection failed to establish")

                    body = json.dumps(
                        {'SeqNo': '1', "Topic": "17." + str(exchange_token), "RType": "OHLCVRANGE", "Action": "req",
                         "Body": json.dumps({"Period": period, "Minutes": falcon_interval, "FromDate": from_date_falcon,
                                             "ToDate": to_date_falcon})})
                    ws.send(body)
                    print("Falcon request body: ", body)

                    try:
                        resp = json.loads(ws.recv())
                        data = resp.get('Body')
                        falcon_data = StringIO(data)
                        final_data = parse_falcon_data(falcon_data)
                        print("************************************************************")
                        print(final_data)
                        print("************************************************************")
                        break  # Exit the loop if the request is successful
                    except websocket._exceptions.WebSocketTimeoutException as e:
                        print(f"WebSocket request timed out (attempt {retries + 1}/{max_retries}): ", e)
                    except Exception as e:
                        print("Exception in receiving Falcon response: ", e)
                    finally:
                        ws.close()  # Ensure the WebSocket connection is closed after use
                except Exception as e:
                    print("Exception in Falcon request: ", e)

                retries += 1
                time.sleep(retry_delay)  # Wait before retrying


        open_match_count = {"Yes": 0, "No": 0}
        high_match_count = {"Yes": 0, "No": 0}
        low_match_count = {"Yes": 0, "No": 0}
        close_match_count = {"Yes": 0, "No": 0}


        comparison_data = []
        for ab_entry in angel_broking_data['data']:
            exfeedtime_ab = ab_entry[0]
            if comparison_type == "Falcon": 
                exfeedtime_ab = datetime.datetime.fromisoformat(exfeedtime_ab).strftime('%Y-%m-%d %H:%M:%S')
            ab_ohlcv = ab_entry[1:5]

            # Find the matching upstox_entry by exfeedtime
            matching_upstox_entry = None
            for upstox_entry in final_data:
                exfeedtime_upstox = upstox_entry[0]
                if comparison_type == "Falcon" and interval=="day": 
                    exfeedtime_upstox = datetime.datetime.strptime(exfeedtime_upstox, '%y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')
                # else:
                #     print(exfeedtime_upstox)
                #     exfeedtime_upstox = datetime.datetime.strptime(exfeedtime_upstox, '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')
                if exfeedtime_ab == exfeedtime_upstox:
                    print(exfeedtime_ab, exfeedtime_upstox)
                    matching_upstox_entry = upstox_entry
                    break


            # If a matching upstox_entry is found, proceed with comparison
            if matching_upstox_entry:
                upstox_ohlcv = matching_upstox_entry[1:]

                # Determine if the OHLCV values match

                open_match = "Yes" if ab_ohlcv[0] == upstox_ohlcv[0] else "No"
                high_match = "Yes" if ab_ohlcv[1] == upstox_ohlcv[1] else "No"
                low_match = "Yes" if ab_ohlcv[2] == upstox_ohlcv[2] else "No"
                close_match = "Yes" if ab_ohlcv[3] == upstox_ohlcv[3] else "No"


                open_match_count[open_match] += 1
                high_match_count[high_match] += 1
                low_match_count[low_match] += 1
                close_match_count[close_match] += 1

                comparison_row = {
                    'exfeedtime': exfeedtime_ab,
                    'isin': isin,
                    'stock_name': stock_name,
                    'angel_open': ab_ohlcv[0],
                    'comparison_open': upstox_ohlcv[0],
                    'open_match': open_match,
                    'angel_high': ab_ohlcv[1],
                    'comparison_high': upstox_ohlcv[1],
                    'high_match': high_match,
                    'angel_low': ab_ohlcv[2],
                    'comparison_low': upstox_ohlcv[2],
                    'low_match': low_match,
                    'angel_close': ab_ohlcv[3],
                    'comparison_close': upstox_ohlcv[3],
                    'close_match': close_match
                }
                comparison_data.append(comparison_row)

        total_counts_row = {
            'open_match_count': open_match_count,
            'high_match_count': high_match_count,
            'low_match_count': low_match_count,
            'close_match_count': close_match_count
        }
        comparison_data.append(total_counts_row)

        return render(request, 'comparison_result.html', {'comparison_data': comparison_data, 
                                                      'comparison_type': comparison_type,
                                                      'open_match_count': open_match_count,
                                                      'high_match_count': high_match_count,
                                                      'low_match_count': low_match_count,
                                                      'close_match_count': close_match_count})

    except Exception as e:
        logger.exception(f"API request failed: {e}")
        return HttpResponse("API request failed.")

    # Fetching the CSV data from the URL
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    response = requests.get(url)

    # Checking if request was successful
    if response.status_code != 200:
        return HttpResponse("Failed to fetch data from Upstox API.")

    # Decompressing and reading the CSV data
    with gzip.open(io.BytesIO(response.content)) as f:
        reader = csv.reader(io.TextIOWrapper(f, encoding='utf-8'))
        next(reader)  # Skipping the header row
        data = [row for row in reader]

    # Filtering NSE_EQ entries and extracting ISIN
    def filter_nse_eq_and_get_isin(data, stock_name):
        nse_eq_entries = [row for row in data if row[2] == stock_name]
        for entry in nse_eq_entries:
            parts = entry[0].split('|')
            if len(parts) == 2:
                return parts[1], entry[1]  # Return ISIN and exchange_token
        return None, None

    # Get ISIN and exchange_token
    isin, exchange_token = filter_nse_eq_and_get_isin(data, stock_name)
    print("isin,exchangetoken",isin,exchange_token)
    # Check if ISIN is fetched successfully
    if isin is None:
        return HttpResponse("Stock not found or not available for comparison.")

    # Angel Broking credentials
    api_key = 'JDldPrre'
    username = 'A912156'
    pwd = '3628'

    # Angel Broking
    smartApi = SmartConnect(api_key)
    try:
        totp = pyotp.TOTP("Z5VGZGBBMT6SWOLCESOEDRLJHA").now()
    except Exception as e:
        logger.error("Invalid Token: The provided token is not valid.")
        raise e

    data = smartApi.generateSession(username, pwd, totp)

    if data['status'] == False:
        logger.error(data)
        return HttpResponse("Failed to authenticate with Angel Broking API.")
    else:
        authToken = data['data']['jwtToken']
        refreshToken = data['data']['refreshToken']
        feedToken = smartApi.getfeedToken()
        res = smartApi.getProfile(refreshToken)
        smartApi.generateToken(refreshToken)
        res = res['data']['exchanges']

    # Fetching data from APIs
    try:
        # Angel Broking API Call
        from_date_angel = from_date + " 00:00"  # Add 09:15 AM as start time for 1 minute or 30 minutes interval
        to_date_angel = to_date + " 15:29"    # Add 03:29 PM as end time for 1 minute or 30 minutes interval

        print("got interval ",interval)
        angel_broking_interval = get_interval_constant(interval, "Angel Broking")
        print("angel interval is ",angel_broking_interval)
        historicParam = {
            "exchange": "NSE",
            "symboltoken": exchange_token,
            "interval": angel_broking_interval,
            "fromdate": from_date_angel,
            "todate": to_date_angel
        }
        angel_broking_data = smartApi.getCandleData(historicParam)
        print(historicParam)
        print("Angel Broking data:", angel_broking_data)
        final_data = ""

        # Upstox API Call
        if comparison_type == "Upstox":
            upstox_interval = get_interval_constant(interval, "Upstox")
            url = f'https://api.upstox.com/v2/historical-candle/NSE_EQ%7C{isin}/{upstox_interval}/{to_date}/{from_date}'
            headers = {
                'Accept': 'application/json'
            }
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                upstox_data = response.json()['data']['candles']
                upstox_data.reverse()
                final_data = upstox_data
                print("usptox data", upstox_data)
            else:
                logger.error(f"Error: {response.status_code} - {response.text}")
                return HttpResponse(f"Error: {response.status_code} - {response.text}")


        if comparison_type == "Falcon":
            falcon_interval = get_interval_constant(interval, "Falcon")
            # fromDate = (datetime.now()+timedelta(fromdatenum)).strftime("%Y-%m-%d")+"T03:46:00.0"
            # toDate = (datetime.now()+timedelta(todatenum)).strftime("%Y-%m-%d")+"T12:00:00.0"
            from_date_falcon = from_date + "T03:46:00.0"  # Add 09:15 AM as start time for 1 minute or 30 minutes interval
            to_date_falcon = to_date + "T12:01:00.0"    # Add 03:29 PM as end time for 1 minute or 30 minutes interval
            print(to_date)
            period = "I"


            if falcon_interval=="0":
                period = "D"

                to_date_obj = datetime.strptime(to_date, "%Y-%m-%d")
                new_date_obj = to_date_obj + timedelta(days=0)
                new_date_str = new_date_obj.strftime("%Y-%m-%d")


                from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
                from_new_date_obj = from_date_obj + timedelta(days=0)
                from_new_date_str = from_new_date_obj.strftime("%Y-%m-%d")


                from_date_falcon = from_new_date_str + "T00:00:00.0"  
                to_date_falcon = new_date_str + "T00:00:00.0"    # Add 03:29 PM as end time for 1 minute or 30 minutes interval

            print(from_date_falcon)
            print(to_date_falcon)
            print(exchange_token)
            print(period)

            
            # ws = create_connection("ws://192.168.253.71:8080")

            ws = create_connection("wss://falconchart.angelbroking.com/")
            print("here")
            try:
                body = json.dumps({'SeqNo': '1',"Topic": "17."+str(999909),"RType": "OHLCVRANGE","Action": "req","Body": json.dumps({"Period":period,"Minutes":falcon_interval,"FromDate":from_date_falcon, "ToDate":to_date_falcon})})
                ws.send(body);
                print("falcon request body " , body)
            except Exception as e:
                print("exception in falcon request : " , e)


            resp = json.loads(ws.recv())
            data = resp.get('Body')
            falcon_data = StringIO(data)

            final_data = parse_falcon_data(falcon_data)
            print("************************************************************")
            print(final_data)
            print("************************************************************")

        # Falcon API CALL


        # Comparison logic
        # comparison_data = []
        # for ab_entry in angel_broking_data['data']:
        #     exfeedtime = ab_entry[0]
        #     ab_ohlcv = ab_entry[1:5]
        #     for upstox_entry in final_data
        #     upstox_ohlcv = upstox_entry[1:]

        #     # Determine if the OHLCV values match
        #     open_match = "Yes" if ab_ohlcv[0] == upstox_ohlcv[0] else "No"
        #     high_match = "Yes" if ab_ohlcv[1] == upstox_ohlcv[1] else "No"
        #     low_match = "Yes" if ab_ohlcv[2] == upstox_ohlcv[2] else "No"
        #     close_match = "Yes" if ab_ohlcv[3] == upstox_ohlcv[3] else "No"

        #     comparison_row = {
        #         'exfeedtime': exfeedtime,
        #         'isin': isin,
        #         'stock_name': stock_name,
        #         'angel_open': ab_ohlcv[0],
        #         'comparison_open': upstox_ohlcv[0],
        #         'open_match': open_match,
        #         'angel_high': ab_ohlcv[1],
        #         'comparison_high': upstox_ohlcv[1],
        #         'high_match': high_match,
        #         'angel_low': ab_ohlcv[2],
        #         'comparison_low': upstox_ohlcv[2],
        #         'low_match': low_match,
        #         'angel_close': ab_ohlcv[3],
        #         'comparison_close': upstox_ohlcv[3],
        #         'close_match': close_match
        #     }
        #     comparison_data.append(comparison_row)
            comparison_data = []
            for ab_entry in angel_broking_data['data']:
                exfeedtime = ab_entry[0]
                ab_ohlcv = ab_entry[1:5]
                print("hiiiiii")

                # Find the matching upstox_entry by exfeedtime
                matching_upstox_entry = None
                for upstox_entry in final_data:
                    if exfeedtime == upstox_entry[0]:
                        matching_upstox_entry = upstox_entry
                        break

                # If a matching upstox_entry is found, proceed with comparison
                if matching_upstox_entry:
                    upstox_ohlcv = matching_upstox_entry[1:]

                    # Determine if the OHLCV values match
                    open_match = "Yes" if ab_ohlcv[0] == upstox_ohlcv[0] else "No"
                    high_match = "Yes" if ab_ohlcv[1] == upstox_ohlcv[1] else "No"
                    low_match = "Yes" if ab_ohlcv[2] == upstox_ohlcv[2] else "No"
                    close_match = "Yes" if ab_ohlcv[3] == upstox_ohlcv[3] else "No"

                    comparison_row = {
                        'exfeedtime': exfeedtime,
                        'isin': isin,
                        'stock_name': stock_name,
                        'angel_open': ab_ohlcv[0],
                        'comparison_open': upstox_ohlcv[0],
                        'open_match': open_match,
                        'angel_high': ab_ohlcv[1],
                        'comparison_high': upstox_ohlcv[1],
                        'high_match': high_match,
                        'angel_low': ab_ohlcv[2],
                        'comparison_low': upstox_ohlcv[2],
                        'low_match': low_match,
                        'angel_close': ab_ohlcv[3],
                        'comparison_close': upstox_ohlcv[3],
                        'close_match': close_match
                    }
                    comparison_data.append(comparison_row)


        return render(request, 'comparison_result.html', {'comparison_data': comparison_data, 'comparison_type':comparison_type})

    except Exception as e:
        logger.exception(f"API request failed: {e}")
        return HttpResponse("API request failed.")

def generate_excel_completeness(request, from_date, to_date, interval, comparison_type, stock_name):
    print("inside" + "generate_excel_completeness" + stock_name)
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    response = requests.get(url)

    # Checking if request was successful
    if response.status_code != 200:
        return HttpResponse("Failed to fetch data from Upstox API.")

    # Decompressing and reading the CSV data
    with gzip.open(io.BytesIO(response.content)) as f:
        reader = csv.reader(io.TextIOWrapper(f, encoding='utf-8'))
        next(reader)  # Skipping the header row
        data = [row for row in reader]

    # Filtering NSE_EQ entries and extracting ISIN
    def filter_nse_eq_and_get_isin(data, stock_name):
        nse_eq_entries = [row for row in data if row[2] == stock_name]
        for entry in nse_eq_entries:
            parts = entry[0].split('|')
            if len(parts) == 2:
                return parts[1], entry[1]  # Return ISIN and exchange_token
        return None, None

    # Get ISIN and exchange_token
    isin, exchange_token = filter_nse_eq_and_get_isin(data, stock_name)
    print("isin,exchangetoken",isin,exchange_token)
    # Check if ISIN is fetched successfully
    if isin is None:
        return HttpResponse("Stock not found or not available for comparison.")

    # Angel Broking credentials
    api_key = 'JDldPrre'
    username = 'A912156'
    pwd = '3628'

    # Angel Broking
    smartApi = SmartConnect(api_key)
    try:
        totp = pyotp.TOTP("Z5VGZGBBMT6SWOLCESOEDRLJHA").now()
    except Exception as e:
        logger.error("Invalid Token: The provided token is not valid.")
        raise e

    data = smartApi.generateSession(username, pwd, totp)

    if data['status'] == False:
        logger.error(data)
        return HttpResponse("Failed to authenticate with Angel Broking API.")
    else:
        authToken = data['data']['jwtToken']
        refreshToken = data['data']['refreshToken']
        feedToken = smartApi.getfeedToken()
        res = smartApi.getProfile(refreshToken)
        smartApi.generateToken(refreshToken)
        res = res['data']['exchanges']

    # Fetching data from APIs
    try:
        # Angel Broking API Call
        from_date_angel = from_date + " 00:00"  # Add 09:15 AM as start time for 1 minute or 30 minutes interval
        to_date_angel = to_date + " 15:29"    # Add 03:29 PM as end time for 1 minute or 30 minutes interval

        print("got interval ",interval)
        angel_broking_interval = get_interval_constant(interval, "Angel Broking")
        print("angel interval is ",angel_broking_interval)
        historicParam = {
            "exchange": "NSE",
            "symboltoken": exchange_token,
            "interval": angel_broking_interval,
            "fromdate": from_date_angel,
            "todate": to_date_angel
        }
        angel_broking_data = smartApi.getCandleData(historicParam)
        print(historicParam)
        print("8888ad8ad8a8da8a")
        print("Angel Broking data:",  angel_broking_data['data'][0],angel_broking_data['data'][-1])
        final_data = ""

        # Upstox API Call
        if comparison_type == "Upstox":
            upstox_interval = get_interval_constant(interval, "Upstox")
            url = f'https://api.upstox.com/v2/historical-candle/NSE_EQ%7C{isin}/{upstox_interval}/{to_date}/{from_date}'
            headers = {
                'Accept': 'application/json'
            }
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                upstox_data = response.json()['data']['candles']
                upstox_data.reverse()
                final_data = upstox_data
                print("upstox data", upstox_data[0],upstox_data[-1])
            else:
                logger.error(f"Error: {response.status_code} - {response.text}")
                return HttpResponse(f"Error: {response.status_code} - {response.text}")


        if comparison_type == "Falcon":
            falcon_interval = get_interval_constant(interval, "Falcon")
            # fromDate = (datetime.now()+timedelta(fromdatenum)).strftime("%Y-%m-%d")+"T03:46:00.0"
            # toDate = (datetime.now()+timedelta(todatenum)).strftime("%Y-%m-%d")+"T12:00:00.0"
            from_date_falcon = from_date + "T03:46:00.0"  # Add 09:15 AM as start time for 1 minute or 30 minutes interval
            to_date_falcon = to_date + "T12:01:00.0"    # Add 03:29 PM as end time for 1 minute or 30 minutes interval
            print(to_date)
            period = "I"


            if falcon_interval=="0":
                period = "D"

                to_date_obj = datetime.strptime(to_date, "%Y-%m-%d")
                new_date_obj = to_date_obj + timedelta(days=0)
                new_date_str = new_date_obj.strftime("%Y-%m-%d")


                from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
                from_new_date_obj = from_date_obj + timedelta(days=0)
                from_new_date_str = from_new_date_obj.strftime("%Y-%m-%d")


                from_date_falcon = from_new_date_str + "T00:00:00.0"  
                to_date_falcon = new_date_str + "T00:00:00.0"    # Add 03:29 PM as end time for 1 minute or 30 minutes interval

            print(from_date_falcon)
            print(to_date_falcon)
            print(exchange_token)
            print(period)

            
            # ws = create_connection("ws://192.168.253.71:8080")

            ws = create_connection("wss://falconchart.angelbroking.com/")
            print("here")
            try:
                body = json.dumps({'SeqNo': '1',"Topic": "17."+str(999909),"RType": "OHLCVRANGE","Action": "req","Body": json.dumps({"Period":period,"Minutes":falcon_interval,"FromDate":from_date_falcon, "ToDate":to_date_falcon})})
                ws.send(body);
                print("falcon request body " , body)
            except Exception as e:
                print("exception in falcon request : " , e)


            resp = json.loads(ws.recv())
            data = resp.get('Body')
            falcon_data = StringIO(data)

            final_data = parse_falcon_data(falcon_data)
            print("************************************************************")

            ##### 

        comparison_data = []
        angel_one_count = len(angel_broking_data['data'])
        upstox_count = len(final_data)

        # Determine if the OHLCV values match
        count_match = "Yes" if angel_one_count == upstox_count else "No"

        comparison_row = {
            'exfeedtime_from': from_date,
            'exfeedtime_to': to_date,
            'isin': isin,
            'stock_name': stock_name,
            'angel_count': angel_one_count,
            'comparison_count': upstox_count,
            'count_match': count_match
        }
        comparison_data.append(comparison_row)


        return render(request, 'completeness_result_stock.html', {'comparison_data': comparison_data, 'comparison_type':comparison_type})

    except Exception as e:
        logger.exception(f"API request failed: {e}")
        return HttpResponse("API request failed.")

# def generate_excel_completeness_segment(request, from_date, to_date, interval, comparison_type,email):
    # print("inside" + "generate_excel_completeness_segment")
    # url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    # response = requests.get(url)

    # # Checking if request was successful
    # if response.status_code != 200:
    #     return HttpResponse("Failed to fetch data from Upstox API.")

    # # Decompressing and reading the CSV data
    # with gzip.open(io.BytesIO(response.content)) as f:
    #     reader = csv.reader(io.TextIOWrapper(f, encoding='utf-8'))
    #     next(reader)  # Skipping the header row
    #     data = [row for row in reader]

    # # Filtering NSE_EQ entries and extracting ISIN
    # all_stocks = {}
    # stock_names = {}
    # def filter_nse_eq_and_get_isin(data):
    #     # print(data)
    #     nse_eq_entries = [row for row in data]
    #     for entry in data:
    #         parts = entry[0].split('|')
    #         if len(parts) == 2:
    #             if parts[0]=="NSE_EQ":
    #                 all_stocks[entry[1]] = parts[1]
    #                 stock_names[entry[1]] = entry[2]
    #             # return parts[1], entry[1]  # Return ISIN and exchange_token
    #     # return None, None

    # # Get ISIN and exchange_token
    # filter_nse_eq_and_get_isin(data)
    # # print("stocks names are ", stock_names)

    # # print(all_stocks)
    # # print("isin,exchangetoken",isin,exchange_token)
    # # Check if ISIN is fetched successfully
    # # if isin is None:
    #     # return HttpResponse("Stock not found or not available for comparison.")

    # # Angel Broking credentials
    # api_key = 'JDldPrre'
    # username = 'A912156'
    # pwd = '3628'
    # # print(all_stocks)

    # # Angel Broking
    # smartApi = SmartConnect(api_key)
    # try:
    #     totp = pyotp.TOTP("Z5VGZGBBMT6SWOLCESOEDRLJHA").now()
    # except Exception as e:
    #     logger.error("Invalid Token: The provided token is not valid.")
    #     raise e

    # data = smartApi.generateSession(username, pwd, totp)

    # if data['status'] == False:
    #     logger.error(data)
    #     return HttpResponse("Failed to authenticate with Angel Broking API.")
    # else:
    #     authToken = data['data']['jwtToken']
    #     refreshToken = data['data']['refreshToken']
    #     feedToken = smartApi.getfeedToken()
    #     res = smartApi.getProfile(refreshToken)
    #     smartApi.generateToken(refreshToken)
    #     res = res['data']['exchanges']

    # # Fetching data from APIs
    # # print(all_stocks)
    # count = 0 
    # total = 0
    # comparison_data = []

    # for index, token in enumerate(tokens_list):

    #     count+=1 
    #     if count==3:
    #         time.sleep(1)
    #         count = 0
    #     if token in all_stocks:
    #         try:
    #             total +=1

    #             progress = (index + 1) / len(tokens_list) * 100
    #             JsonResponse({'progress': progress})

    #             print("*****")
    #             print(token,all_stocks[token])
    #             print("*****")
    #             # Angel Broking API Call
    #             from_date_angel = from_date + " 00:00"  # Add 09:15 AM as start time for 1 minute or 30 minutes interval
    #             to_date_angel = to_date + " 15:29"    # Add 03:29 PM as end time for 1 minute or 30 minutes interval

    #             print("got interval ",interval)
    #             angel_broking_interval = get_interval_constant(interval, "Angel Broking")
    #             print("angel interval is ",angel_broking_interval)
    #             historicParam = {
    #                 "exchange": "NSE",
    #                 "symboltoken": token,
    #                 "interval": angel_broking_interval,
    #                 "fromdate": from_date_angel,
    #                 "todate": to_date_angel
    #             }
    #             angel_broking_data = smartApi.getCandleData(historicParam)
    #             # print("**" + angel_broking_data['data'] + "**")
    #             # if angel_broking_data is None:
    #             #     print("data is ")
    #             #     continue
    #             print(angel_broking_data)
    #             print(historicParam)
    #             print("8888ad8ad8a8da8a")
    #             print("Angel Broking data:",  angel_broking_data['data'][0],angel_broking_data['data'][-1])
    #             final_data = ""

    #             # Upstox API Call
    #             if comparison_type == "Upstox":
    #                 upstox_interval = get_interval_constant(interval, "Upstox")
    #                 url = f'https://api.upstox.com/v2/historical-candle/NSE_EQ%7C{all_stocks[token]}/{upstox_interval}/{to_date}/{from_date}'
    #                 headers = {
    #                     'Accept': 'application/json'
    #                 }
    #                 response = requests.get(url, headers=headers)
    #                 if response.status_code == 200:
    #                     upstox_data = response.json()['data']['candles']
    #                     upstox_data.reverse()
    #                     final_data = upstox_data
    #                     print("upstox data", upstox_data[0],upstox_data[-1])
    #                 else:
    #                     logger.error(f"Error: {response.status_code} - {response.text}")
    #                     return HttpResponse(f"Error: {response.status_code} - {response.text}")


    #             if comparison_type == "Falcon":
    #                 falcon_interval = get_interval_constant(interval, "Falcon")
    #                 # fromDate = (datetime.now()+timedelta(fromdatenum)).strftime("%Y-%m-%d")+"T03:46:00.0"
    #                 # toDate = (datetime.now()+timedelta(todatenum)).strftime("%Y-%m-%d")+"T12:00:00.0"
    #                 from_date_falcon = from_date + "T03:46:00.0"  # Add 09:15 AM as start time for 1 minute or 30 minutes interval
    #                 to_date_falcon = to_date + "T12:01:00.0"    # Add 03:29 PM as end time for 1 minute or 30 minutes interval
    #                 print(to_date)
    #                 period = "I"


    #                 if falcon_interval=="0":
    #                     period = "D"

    #                     to_date_obj = datetime.strptime(to_date, "%Y-%m-%d")
    #                     new_date_obj = to_date_obj + timedelta(days=0)
    #                     new_date_str = new_date_obj.strftime("%Y-%m-%d")


    #                     from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
    #                     from_new_date_obj = from_date_obj + timedelta(days=0)
    #                     from_new_date_str = from_new_date_obj.strftime("%Y-%m-%d")


    #                     from_date_falcon = from_new_date_str + "T00:00:00.0"  
    #                     to_date_falcon = new_date_str + "T00:00:00.0"    # Add 03:29 PM as end time for 1 minute or 30 minutes interval

    #                 print(from_date_falcon)
    #                 print(to_date_falcon)
    #                 print(exchange_token)
    #                 print(period)

                    
    #                 # ws = create_connection("ws://192.168.253.71:8080")

    #                 ws = create_connection("wss://falconchart.angelbroking.com/")
    #                 print("here")
    #                 try:
    #                     body = json.dumps({'SeqNo': '1',"Topic": "17."+str(999909),"RType": "OHLCVRANGE","Action": "req","Body": json.dumps({"Period":period,"Minutes":falcon_interval,"FromDate":from_date_falcon, "ToDate":to_date_falcon})})
    #                     ws.send(body);
    #                     print("falcon request body " , body)
    #                 except Exception as e:
    #                     print("exception in falcon request : " , e)


    #                 resp = json.loads(ws.recv())
    #                 data = resp.get('Body')
    #                 falcon_data = StringIO(data)

    #                 final_data = parse_falcon_data(falcon_data)
    #                 print("************************************************************")

    #                 ##### 

    #             angel_one_count = len(angel_broking_data['data'])
    #             upstox_count = len(final_data)

    #             # Determine if the OHLCV values match
    #             count_match = "Yes" if angel_one_count == upstox_count else "No"

    #             comparison_row = {
    #                 'exfeedtime_from': from_date,
    #                 'exfeedtime_to': to_date,
    #                 'isin': all_stocks[token],
    #                 'stock_name': stock_names[token],
    #                 'angel_count': angel_one_count,
    #                 'comparison_count': upstox_count,
    #                 'count_match': count_match
    #             }
    #             comparison_data.append(comparison_row)


    #         except Exception as e:
    #             logger.exception(f"API request failed: {e}")
    #             continue
    #             # return HttpResponse("API request failed.")





    # return render(request, 'completeness_result.html', {'comparison_data': comparison_data, 'comparison_type':comparison_type})




def get_latest_file(broker):
    date = datetime.now() - timedelta(days=0)

    while True:
        if broker == 'angel':
            key =  f'comparison_completeness_tool_job/angel_one_data/nse_equity/1_day/{date.strftime("%Y-%m-%d")}.part_00000'
        else:
            key = f'comparison_completeness_tool_job/upstox_data/nse_equity/1_day/{date.strftime("%Y-%m-%d")}.csv'
            


        try:
            print(key)
            obj = s3.get_object(Bucket='angel-server-data-dev', Key=key)
            df = pd.read_csv(obj['Body'])
            print(f"Found file for date: {date.strftime('%Y-%m-%d')}")
            return df
        except s3.exceptions.NoSuchKey:
            print(f"File not found for date: {date.strftime('%Y-%m-%d')}, trying previous day.")
            date -= timedelta(days=1)
        except Exception as e:
            print(f"An error occurred: {e}")
            break


def generate_excel_comparison_segment(request, from_date, to_date, interval, comparison_type, ohlcv, threshold):
    print("inside generate_excel_completeness_segment")

    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    response = requests.get(url)

    if response.status_code != 200:
        return HttpResponse("Failed to fetch data from Upstox API.")

    with gzip.open(io.BytesIO(response.content)) as f:
        reader = csv.reader(io.TextIOWrapper(f, encoding='utf-8'))
        next(reader)  # Skipping the header row
        data = [row for row in reader]

    all_stocks = {}
    stock_names = {}
    
    def filter_nse_eq_and_get_isin(data):
        nse_eq_entries = [row for row in data]
        for entry in data:
            parts = entry[0].split('|')
            if len(parts) == 2 and parts[0] == "NSE_EQ":
                all_stocks[entry[1]] = parts[1]
                stock_names[entry[1]] = entry[2]

    filter_nse_eq_and_get_isin(data)

    angel_data = get_latest_file('angel')
    upstox_data = get_latest_file('upstox')

    upstox_data.columns = ['index', 'token', 'isin', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
    angel_data.columns = ['token', 'timestamp', 'open', 'high', 'low', 'close', 'volume']

    angel_data['timestamp'] = pd.to_datetime(angel_data['timestamp'], errors='coerce')
    upstox_data['timestamp'] = pd.to_datetime(upstox_data['timestamp'], errors='coerce')

    angel_data.dropna(subset=['timestamp'], inplace=True)
    upstox_data.dropna(subset=['timestamp'], inplace=True)

    angel_data['timestamp'] = angel_data['timestamp'].dt.tz_localize(None)
    upstox_data['timestamp'] = upstox_data['timestamp'].dt.tz_localize(None)

    from_date_dt = pd.to_datetime(from_date)
    to_date_dt = pd.to_datetime(to_date)

    angel_data_filtered = angel_data[(angel_data['timestamp'] >= from_date_dt) & (angel_data['timestamp'] <= to_date_dt)]
    upstox_data_filtered = upstox_data[(upstox_data['timestamp'] >= from_date_dt) & (upstox_data['timestamp'] <= to_date_dt)]
    angel_counts = angel_data_filtered['token'].value_counts().to_dict()
    upstox_counts = upstox_data_filtered['token'].value_counts().to_dict()
    comparison_data = []

    # print(angel_data_filtered.head(10))
    # print(upstox_data_filtered.head(10))

    # for token in angel_counts.keys() & upstox_counts.keys():

    for token in angel_counts.keys() & upstox_counts.keys():
        angel_filtered = angel_data_filtered[angel_data_filtered['token'] == token]
        upstox_filtered = upstox_data_filtered[upstox_data_filtered['token'] == token]

        mismatches = 0
        for _, angel_row in angel_filtered.iterrows():
            corresponding_upstox = upstox_filtered[upstox_filtered['timestamp'] == angel_row['timestamp']]
            if not corresponding_upstox.empty:
                upstox_row = corresponding_upstox.iloc[0]
                # print(angel_row[ohlcv], upstox_row[ohlcv])
                if abs(angel_row[ohlcv] - upstox_row[ohlcv]) > 0:
                    mismatches += 1

        if mismatches > int(threshold):
            comparison_row = {
                'exfeedtime_from': from_date,
                'exfeedtime_to': to_date,
                'isin': all_stocks.get(str(token), token),
                'stock_name': stock_names.get(str(token), token),
                'mismatches': mismatches,
                'token': token
            }
            comparison_data.append(comparison_row)

    request.session['angel_data_filtered'] = base64.b64encode(pickle.dumps(angel_data_filtered)).decode('utf-8')
    request.session['upstox_data_filtered'] = base64.b64encode(pickle.dumps(upstox_data_filtered)).decode('utf-8')

    return render(request, 'comparison_result_segment.html', {'comparison_data': comparison_data, 'comparison_type': comparison_type})


def generate_excel_completeness_segment(request, from_date, to_date, interval, comparison_type, email):
    print("inside generate_excel_completeness_segment")

    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    response = requests.get(url)

    # Checking if request was successful
    if response.status_code != 200:
        return HttpResponse("Failed to fetch data from Upstox API.")

    # Decompressing and reading the CSV data
    with gzip.open(io.BytesIO(response.content)) as f:
        reader = csv.reader(io.TextIOWrapper(f, encoding='utf-8'))
        next(reader)  # Skipping the header row
        data = [row for row in reader]

    # Filtering NSE_EQ entries and extracting ISIN
    all_stocks = {}
    stock_names = {}
    def filter_nse_eq_and_get_isin(data):
        # print(data)
        nse_eq_entries = [row for row in data]
        for entry in data:
            parts = entry[0].split('|')
            if len(parts) == 2:
                if parts[0]=="NSE_EQ":
                    all_stocks[entry[1]] = parts[1]
                    stock_names[entry[1]] = entry[2]
                # return parts[1], entry[1]  # Return ISIN and exchange_token
        # return None, None
    filter_nse_eq_and_get_isin(data)

    s3_bucket = 'angel-server-data-dev'
    # angel_path = f'comparison_completeness_tool_job/angel_one_data/nse_equity/1_day/{datetime.today().strftime("%Y-%m-%d")}.part_00000'
    # upstox_path = f'comparison_completeness_tool_job/upstox_data/nse_equity/1_day/{datetime.today().strftime("%Y-%m-%d")}.csv'
    
    # print(angel_path)
    # print(upstox_path)

    # Initialize S3 client
    # s3 = boto3.client('s3')

    # Read the Angel data
    # angel_obj = s3.get_object(Bucket=s3_bucket, Key=angel_path)
    # angel_data = pd.read_csv(angel_obj['Body'], sep=',')  # Adjust separator if needed

    # Read the Upstox data
    # upstox_obj = s3.get_object(Bucket=s3_bucket, Key=upstox_path)
    # upstox_data = pd.read_csv(upstox_obj['Body'])

    angel_data = get_latest_file('angel')
    upstox_data = get_latest_file('upstox')




    # Ensure both datasets have the same column names and types
    upstox_data.columns = ['index','token', 'isin', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
    angel_data.columns = ['token', 'timestamp', 'open', 'high', 'low', 'close', 'volume']

    # dfangelprint = angel_data[angel_data['token'] == 21252]
    # dfupstoxprint = upstox_data[upstox_data['token'] == 21252]
    # print(dfangelprint)
    # print(dfupstoxprint)

    # Convert timestamps to datetime and normalize
    angel_data['timestamp'] = pd.to_datetime(angel_data['timestamp'], errors='coerce')
    upstox_data['timestamp'] = pd.to_datetime(upstox_data['timestamp'], errors='coerce')

    # Drop rows with invalid timestamps
    angel_data.dropna(subset=['timestamp'], inplace=True)
    upstox_data.dropna(subset=['timestamp'], inplace=True)

    # Normalize timezones (assuming timestamps have timezone info, otherwise remove `.dt.tz_localize(None)`)
    angel_data['timestamp'] = angel_data['timestamp'].dt.tz_localize(None)
    upstox_data['timestamp'] = upstox_data['timestamp'].dt.tz_localize(None)

    print("here2")
    # Filter data between from_date and to_date
    from_date_dt = pd.to_datetime(from_date)
    to_date_dt = pd.to_datetime(to_date)

    print("here2")
    angel_data_filtered = angel_data[(angel_data['timestamp'] >= from_date_dt) & (angel_data['timestamp'] <= to_date_dt)]
    upstox_data_filtered = upstox_data[(upstox_data['timestamp'] >= from_date_dt) & (upstox_data['timestamp'] <= to_date_dt)]

    # Count the entries for each token
    angel_counts = angel_data_filtered['token'].value_counts().to_dict()
    upstox_counts = upstox_data_filtered['token'].value_counts().to_dict()
    print(angel_counts)
    print(angel_counts)
    comparison_data = []
    # print(stock_names)
    # print(all_stocks)
    total = 0 
    for token in angel_counts.keys() & upstox_counts.keys():
        # total+=1 
        # if total > 15:
        #     break
        angel_count = angel_counts.get(token, 0)
        upstox_count = upstox_counts.get(token, 0)
        count_match = "Yes" if angel_count == upstox_count else "No"
        difference = abs(angel_count - upstox_count)

        comparison_row = {
            'exfeedtime_from': from_date,
            'exfeedtime_to': to_date,
            'isin': all_stocks.get(str(token), token),
            'stock_name': stock_names.get(str(token), token),
            'angel_count': angel_count,
            'comparison_count': upstox_count,
            'count_match': count_match,
            'difference': difference,  # Add difference to the row
            'token': token
        }
        comparison_data.append(comparison_row)

    request.session['angel_data_filtered'] = base64.b64encode(pickle.dumps(angel_data_filtered)).decode('utf-8')
    request.session['upstox_data_filtered'] = base64.b64encode(pickle.dumps(upstox_data_filtered)).decode('utf-8')


    return render(request, 'completeness_result.html', {'comparison_data': comparison_data, 'comparison_type': comparison_type})

def detailed_comparison(request, token, stock_name):
    # s3_bucket = 'angel-server-data-dev'
    # angel_path = f'comparison_completeness_tool_job/angel_one_data/nse_equity/1_day/{datetime.today().strftime("%Y-%m-%d")}.part_00000'
    # upstox_path = f'comparison_completeness_tool_job/upstox_data/nse_equity/1_day/{datetime.today().strftime("%Y-%m-%d")}.csv'
    
    # # Initialize S3 client
    # s3 = boto3.client('s3')

    # # Read the Angel data
    # angel_obj = s3.get_object(Bucket=s3_bucket, Key=angel_path)
    # angel_data = pd.read_csv(angel_obj['Body'], sep=',', header=None)  # Adjust separator if needed

    # # Read the Upstox data
    # upstox_obj = s3.get_object(Bucket=s3_bucket, Key=upstox_path)
    # upstox_data = pd.read_csv(upstox_obj['Body'])

    # # Convert timestamps to datetime
    # upstox_data.columns = ['index','token', 'isin', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
    # angel_data.columns = ['token', 'timestamp', 'open', 'high', 'low', 'close', 'volume']

    # angel_data['timestamp'] = pd.to_datetime(angel_data['timestamp'], errors='coerce')
    # upstox_data['timestamp'] = pd.to_datetime(upstox_data['timestamp'], errors='coerce')

    # # Drop rows with invalid timestamps
    # angel_data.dropna(subset=['timestamp'], inplace=True)
    # upstox_data.dropna(subset=['timestamp'], inplace=True)

    # # Normalize timezones (assuming timestamps have timezone info, otherwise remove `.dt.tz_localize(None)`)
    # angel_data['timestamp'] = angel_data['timestamp'].dt.tz_localize(None)
    # upstox_data['timestamp'] = upstox_data['timestamp'].dt.tz_localize(None)

    # print("here2")
    # # Filter data between from_date and to_date
    # from_date_dt = pd.to_datetime(from_date)
    # to_date_dt = pd.to_datetime(to_date)

    # print("here2")
    # angel_data_filtered = angel_data[(angel_data['timestamp'] >= from_date_dt) & (angel_data['timestamp'] <= to_date_dt)]
    # upstox_data_filtered = upstox_data[(upstox_data['timestamp'] >= from_date_dt) & (upstox_data['timestamp'] <= to_date_dt)]

    # Filter the data for the specific token


    angel_data_filtered = pickle.loads(base64.b64decode(request.session.get('angel_data_filtered', b'')))
    upstox_data_filtered = pickle.loads(base64.b64decode(request.session.get('upstox_data_filtered', b'')))
    # if filtered_dataframes:
    #     angel_data_filtered = filtered_dataframes['angel_data_filtered']
    #     upstox_data_filtered = filtered_dataframes['upstox_data_filtered']
    # if angel_data_filtered and upstox_data_filtered:

    print(angel_data_filtered.head(10))
    print(upstox_data_filtered.head(10))

    angel_filtered = angel_data_filtered[angel_data_filtered['token'] == int(token)]
    upstox_filtered = upstox_data_filtered[upstox_data_filtered['token'] == int(token)]

    # Find missing candles
    angel_timestamps = set(angel_filtered['timestamp'])
    upstox_timestamps = set(upstox_filtered['timestamp'])

    missing_in_upstox = angel_filtered[~angel_filtered['timestamp'].isin(upstox_timestamps)]
    missing_in_angel = upstox_filtered[~upstox_filtered['timestamp'].isin(angel_timestamps)]

    # Prepare the data for rendering
    missing_candles = []

    for _, row in missing_in_upstox.iterrows():
        missing_candles.append({
                        'token': row['token'],
            'timestamp': row['timestamp'],
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': row['close'],
            'volume': row['volume'],
            'source': 'Angel'
        })

    for _, row in missing_in_angel.iterrows():
        missing_candles.append({
            'token': row['token'],
            'timestamp': row['timestamp'],
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': row['close'],
            'volume': row['volume'],
            'source': 'Upstox'
        })

    return render(request, 'detailed_comparison.html', {'missing_candles': missing_candles, 'stock_name': stock_name})

    # else:
    # #     # Handle the case where filtered dataframes are not available
    #     return render(request, 'error.html', {'message': 'Filtered dataframes are not available.'})
def comparison_form(request):
    progress = cache.get('progress') or 0  # Get progress from cache or default to 0

    if request.method == 'POST':
        stock_name = request.POST.get('stock_name')
        from_date = request.POST.get('from_date')
        to_date = request.POST.get('to_date')
        interval = request.POST.get('timeframe')
        comparison_type = request.POST.get('comparison_type')
        form_type = request.POST.get('form_type')
        email = request.POST.get('email')
        ohlcv = request.POST.get('OHLCV')
        threshold = request.POST.get('mismatch_threshold')

        print(stock_name,from_date,to_date,interval,comparison_type,form_type,email,ohlcv,threshold)
        # Validate input data (ensure all fields are filled)
        if form_type=="comparison":
            if not all([from_date, to_date, interval]):
                return render(request, 'error.html', {'message': "Please fill in all fields."})
        else:
            if not all([ from_date, to_date, interval]):
                return render(request, 'error.html', {'message': "Please fill in all fields."})

        # Make surea interval is one of the supported values
        valid_intervals = ["1minute", "30minute", "day"]
        if interval not in valid_intervals:
            return render(request, 'error.html', {'message': "Invalid interval value."})

        if form_type=="comparison":
            if stock_name != "":
                return generate_excel(request, stock_name, from_date, to_date, interval,comparison_type)
            else:
                return generate_excel_comparison_segment(request, from_date, to_date, interval,comparison_type,ohlcv,threshold)
        elif stock_name == "":
            print("hererererere")
            return generate_excel_completeness_segment(request, from_date, to_date, interval,comparison_type,email)
        else:
            return generate_excel_completeness(request, from_date, to_date, interval,comparison_type,stock_name)


        # Pass the input data to the generate_excel view for processing

    else:
        # Fetch the CSV file and extract stock names
        url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
        response = requests.get(url)
        
        if response.status_code != 200:
            return render(request, 'error.html', {'message': "Failed to fetch data from Upstox API."})

        with gzip.open(io.BytesIO(response.content)) as f:
            reader = csv.reader(io.TextIOWrapper(f, encoding='utf-8'))
            next(reader)  # Skip header
            stock_names = [row[2] for row in reader]  # Assuming stock name is in the third column

        return render(request, 'comparison_form.html', {'stock_names': stock_names})
