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
session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    aws_session_token=os.environ["AWS_SESSION_TOKEN"],
)



# Test it on a service (yours may be different)

s3 = boto3.client('s3')
obj = s3.get_object(Bucket='angel-server-data-production', Key='chart-data-backup/nse_equity/master_token/2024-06-07/nse_equity_tokenmaster.csv')
df = pd.read_csv(obj['Body'])
print(df.head(20))


angel_path = f's3://angel-server-data-dev/comparison_completeness_tool_job/angel_one_data/nse_equity/1_day/2024-06-10.part_00000'


# Initialize S3 client
s3 = boto3.client('s3')

# Read the Angel data
angel_obj = s3.get_object(Bucket=s3_bucket, Key=angel_path)
angel_data = pd.read_csv(angel_obj['Body'], sep=',')  # Adjust separator if needed
angel_data.columns = ['token', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
angel_data[angel_data['token'] == 14058]
print(angel_data.head(10))