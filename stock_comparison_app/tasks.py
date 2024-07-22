# tasks.py
from celery import shared_task
from django.core.mail import EmailMessage
from django.conf import settings
from openpyxl import Workbook
import pandas as pd
import requests
import gzip
import io
import csv
import pyotp
from logzero import logger
from websocket import create_connection
import json
from io import StringIO
from datetime import datetime, timedelta
import time
from SmartApi import SmartConnect
import pandas as pd
import boto3
import logging
logger = logging.getLogger(__name__)

s3 = boto3.client('s3')
obj = s3.get_object(Bucket='angel-server-data-production', Key='chart-data-backup/nse_equity/master_token/' + datetime.now().strftime(
            "%Y-%m-%d") + '/nse_equity_isin_token.csv')
df = pd.read_csv(obj['Body'])
print(df.head(20))
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


@shared_task
def generate_and_send_completeness_report(from_date, to_date, interval, comparison_type, email):
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    response = requests.get(url)

    if response.status_code != 200:
        return "Failed to fetch data from Upstox API."

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

    api_key = 'JDldPrre'
    username = 'A912156'
    pwd = '3628'

    smartApi = SmartConnect(api_key)
    try:
        totp = pyotp.TOTP("Z5VGZGBBMT6SWOLCESOEDRLJHA").now()
    except Exception as e:
        logger.error("Invalid Token: The provided token is not valid.")
        raise e

    data = smartApi.generateSession(username, pwd, totp)

    if not data['status']:
        logger.error(data)
        return "Failed to authenticate with Angel Broking API."
    else:
        authToken = data['data']['jwtToken']
        refreshToken = data['data']['refreshToken']
        feedToken = smartApi.getfeedToken()
        res = smartApi.getProfile(refreshToken)
        smartApi.generateToken(refreshToken)
        res = res['data']['exchanges']

    tokens_list = [str(row['token']) for index, row in df.iterrows()]
    # tokens_list = tokens_list[0:20]
    count = 0
    total = 0
    comparison_data = []
    tokens_list = tokens_list[0:50]
    logger.info(tokens_list)
    for index, token in enumerate(tokens_list):
        logger.info(index)
        count += 1
        if count == 3:
            time.sleep(1)
            count = 0
        if token in all_stocks:
            try:
                total += 1

                from_date_angel = from_date + " 00:00"
                to_date_angel = to_date + " 15:29"

                angel_broking_interval = get_interval_constant(interval, "Angel Broking")
                historicParam = {
                    "exchange": "NSE",
                    "symboltoken": token,
                    "interval": angel_broking_interval,
                    "fromdate": from_date_angel,
                    "todate": to_date_angel
                }
                angel_broking_data = smartApi.getCandleData(historicParam)
                logger.info("angel broking data")
                logger.info(angel_broking_data)

                if comparison_type == "Upstox":
                    upstox_interval = get_interval_constant(interval, "Upstox")
                    url = f'https://api.upstox.com/v2/historical-candle/NSE_EQ%7C{all_stocks[token]}/{upstox_interval}/{to_date}/{from_date}'
                    headers = {'Accept': 'application/json'}
                    response = requests.get(url, headers=headers)
                    if response.status_code == 200:
                        upstox_data = response.json()['data']['candles']
                        upstox_data.reverse()
                        final_data = upstox_data
                    else:
                        logger.error(f"Error: {response.status_code} - {response.text}")
                        return f"Error: {response.status_code} - {response.text}"

                if comparison_type == "Falcon":
                    falcon_interval = get_interval_constant(interval, "Falcon")
                    from_date_falcon = from_date + "T03:46:00.0"
                    to_date_falcon = to_date + "T12:01:00.0"
                    period = "I"
                    if falcon_interval == "0":
                        period = "D"
                        to_date_obj = datetime.strptime(to_date, "%Y-%m-%d")
                        new_date_obj = to_date_obj + timedelta(days=0)
                        new_date_str = new_date_obj.strftime("%Y-%m-%d")
                        from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
                        from_new_date_obj = from_date_obj + timedelta(days=0)
                        from_new_date_str = from_new_date_obj.strftime("%Y-%m-%d")
                        from_date_falcon = from_new_date_str + "T00:00:00.0"
                        to_date_falcon = new_date_str + "T00:00:00.0"

                    ws = create_connection("wss://falconchart.angelbroking.com/")
                    try:
                        body = json.dumps({
                            'SeqNo': '1',
                            "Topic": "17." + str(999909),
                            "RType": "OHLCVRANGE",
                            "Action": "req",
                            "Body": json.dumps({
                                "Period": period,
                                "Minutes": falcon_interval,
                                "FromDate": from_date_falcon,
                                "ToDate": to_date_falcon
                            })
                        })
                        ws.send(body)
                    except Exception as e:
                        print("exception in falcon request: ", e)

                    resp = json.loads(ws.recv())
                    data = resp.get('Body')
                    falcon_data = StringIO(data)
                    final_data = parse_falcon_data(falcon_data)

                angel_one_count = len(angel_broking_data['data'])
                upstox_count = len(final_data)
                count_match = "Yes" if angel_one_count == upstox_count else "No"

                comparison_row = {
                    'exfeedtime_from': from_date,
                    'exfeedtime_to': to_date,
                    'isin': all_stocks[token],
                    'stock_name': stock_names[token],
                    'angel_count': angel_one_count,
                    'comparison_count': upstox_count,
                    'count_match': count_match
                }
                comparison_data.append(comparison_row)
            except Exception as e:
                logger.exception(f"API request failed: {e}")
                continue

    wb = Workbook()
    ws = wb.active
    ws.title = "Comparison Data"
    headers = ['exfeedtime_from', 'exfeedtime_to', 'isin', 'stock_name', 'angel_count', 'comparison_count', 'count_match']
    ws.append(headers)
    for row in comparison_data:
        ws.append([row['exfeedtime_from'], row['exfeedtime_to'], row['isin'], row['stock_name'], row['angel_count'], row['comparison_count'], row['count_match']])

    excel_file = io.BytesIO()
    wb.save(excel_file)
    excel_file.seek(0)

    subject = 'Comparison Data Result'
    message = 'Please find attached the comparison data result.'
    from_email = settings.DEFAULT_FROM_EMAIL
    recipient_list = [email]

    email_message = EmailMessage(subject, message, from_email, recipient_list)
    email_message.attach('comparison_data.xlsx', excel_file.read(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    email_message.send()

    return HttpResponse("Comparison data has been sent to the specified email address.")
