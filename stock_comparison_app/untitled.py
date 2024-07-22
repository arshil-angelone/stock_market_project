from SmartApi import SmartConnect

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