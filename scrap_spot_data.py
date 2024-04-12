import csv
from datetime import datetime
import os
import arrow
from ib_insync import IB, Contract
import pytz
from termcolor import cprint

'''
    This function gets historical data of the spot contract from the previous trading day.
    eg: start_date = '2024-04-01', this function will collect trading data 1 trading day pior to `2024-04-01 8:00 am`
    note: start_date and end_date should be STRICTLY in the format 'YYYY-MM-DD'
'''

def get_spot_trading_data(tz, start_date, end_date):
    start_time = arrow.get(start_date, 'YYYY-MM-DD').replace(hour=8,tzinfo=tz)
    end_time = arrow.get(end_date, 'YYYY-MM-DD').replace(hour=7,tzinfo=tz)

    data_dir = "data"
    # path_spot = os.path.join(data_dir, f'HSIF_spot_{start_date}_{end_date}.csv')
    
    ib = IB()
    ib.connect("127.0.0.1", 4002, clientId=1)
    contract_spot = Contract()
    contract_spot.symbol = "HSI"
    contract_spot.secType = "FUT"
    contract_spot.exchange = "HKFE"
    contract_spot.currency = "HKD"
    contract_spot.includeExpired = True
    contract_spot.lastTradeDateOrContractMonth = start_time.format('YYYYMM')

    last_trading_date_spot = ib.reqContractDetails(contract_spot)[0].contract.lastTradeDateOrContractMonth
    file_store_path = os.path.join(data_dir, f'HSIF_spot_{start_time.format("YYYYMM")}.csv')

    secondsInOneDay = 24 * 60 * 60
    for i in range(int(start_time.timestamp()), int(end_time.timestamp()), secondsInOneDay):
        tarding_date = arrow.get(i).to(tz)
        if tarding_date.weekday() in [5, 6]:
            # avoid grabbing data on weekends
            # TODO: avoid grabbing data on public holidays
            cprint(f"{tarding_date} is a weekend", 'yellow')
            pass
        else:
            if int(tarding_date.format('YYYYMMDD')) <= int(last_trading_date_spot):
                cprint(f"{tarding_date} is before lastTradingDateSpot:{last_trading_date_spot}", 'green')
            else:
                cprint(f"{tarding_date} is after lastTradingDateSpot:{last_trading_date_spot}, updating lastTradingDate....", 'red')
                contract_spot.lastTradeDateOrContractMonth = tarding_date.format('YYYYMM')
                last_trading_date_spot = ib.reqContractDetails(contract_spot)[0].contract.lastTradeDateOrContractMonth
                file_store_path = os.path.join(data_dir, f'HSIF_spot_{tarding_date.format("YYYYMM")}.csv')

            # get trade bars of yesterday of dateOfIter
            bars = ib.reqHistoricalData(
                contract_spot,
                endDateTime = datetime.fromtimestamp(i, pytz.timezone(tz)),
                durationStr = "57600 S",
                barSizeSetting="30 mins",
                whatToShow="TRADES",
                useRTH=False,  # True: Regular trading hours only
                formatDate=2,
            )

            if not os.path.exists(file_store_path):
                os.makedirs(data_dir, exist_ok=True)
                with open(file_store_path, "w", newline="") as file:
                    writer = csv.writer(file)
                    writer.writerow(
                        [
                            "Time",
                            "Open",
                            "High",
                            "Low",
                            "Close",
                            "Volume",
                            "LastTradingDate",
                            "TimeStamp",
                            "forTradingDay"
                            "forTradingDay_Timestamp"
                        ]
                    )
                file.close()

            with open(file_store_path, "a", newline="") as file:
                writer = csv.writer(file)
                for bar in bars:
                    writer.writerow(
                        [
                            bar.date.astimezone(pytz.timezone(tz)),
                            bar.open,
                            bar.high,
                            bar.low,
                            bar.close,
                            int(bar.volume),
                            last_trading_date_spot,
                            int(bar.date.timestamp()),
                            tarding_date.format('YYYY-MM-DD'),
                            int(tarding_date.timestamp())
                        ]
                    )
            file.close()    
    return




get_spot_trading_data('Asia/Hong_Kong', '2024-01-20', '2024-02-05')