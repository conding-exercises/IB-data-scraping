from termcolor import cprint
import os
import csv
from datetime import datetime
import pytz
from ib_insync import IB, Contract


hong_kong_tz = pytz.timezone("Asia/Hong_Kong")
data_dir = "data"
path_spot = os.path.join(data_dir, "HSIF_spot.csv")

#  ************ time frame ************
startingDate = datetime(2024, 3, 25, 8, 0, 0, 0, hong_kong_tz)
today = datetime.now(hong_kong_tz).replace(hour=7, minute=0, second=0, microsecond=0)



def writeCsvSpot(contract, file_path):
    # ************ Create an IB object ************
    os.makedirs(data_dir, exist_ok=True)
    with open(file_path, "w", newline="") as file:
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
            ]
        )
        # Write the historical data
        for bar in contract:
            writer.writerow(
                [
                    bar.date.astimezone(hong_kong_tz),
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    int(bar.volume),
                    lastTradingDateSpot,
                    int(bar.date.timestamp()),
                ]
            )
    return


def updateCsvSpot(contract, file_path):
    with open(file_path, "a", newline="") as file:
        writer = csv.writer(file)
        for bar in contract:
            writer.writerow(
                [
                    bar.date.astimezone(hong_kong_tz),
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    int(bar.volume),
                    lastTradingDateSpot,
                    int(bar.date.timestamp()),
                ]
            )
    return


ib = IB()
ib.connect("127.0.0.1", 4001, clientId=1)

contractSpot = Contract()
contractSpot.symbol = "HSI"
contractSpot.secType = "FUT"
contractSpot.exchange = "HKFE"
contractSpot.currency = "HKD"
contractSpot.includeExpired = True
contractSpot.lastTradeDateOrContractMonth = str(startingDate.year) + str(startingDate.month).zfill(2)

# contract example: contract=Contract(secType='FUT', conId=675123242, symbol='HSI', lastTradeDateOrContractMonth='20240429', multiplier='50', exchange='HKFE', currency='HKD', localSymbol='HSIJ4', tradingClass='HSI')
lastTradingDateSpot = ib.reqContractDetails(contractSpot)[0].contract.lastTradeDateOrContractMonth
cprint(lastTradingDateSpot, "green")


secondsInOneDay = 24 * 60 * 60
for i in range(int(startingDate.timestamp()), int(today.timestamp()), secondsInOneDay):
    dateOfIter = datetime.fromtimestamp(i)
    if int(dateOfIter.strftime("%Y%m%d")) <= int(lastTradingDateSpot):
        # normal
        cprint(f"{dateOfIter} is before lastTradingDateSpot","green")
    else:
        # update the spot contract with next month
        cprint(f"{dateOfIter} is after lastTradingDateSpot, updating contract month...","red")
        contractSpot.lastTradeDateOrContractMonth = str(dateOfIter.year) + str(
            dateOfIter.month + 1
        ).zfill(2)
        lastTradingDateSpot = ib.reqContractDetails(contractSpot)[0].contract.lastTradeDateOrContractMonth

    # get trade bars of yesterday of dateOfIter
    bars = ib.reqHistoricalData(
        contractSpot,
        endDateTime=dateOfIter,
        durationStr="57600 S",
        barSizeSetting="30 mins",
        whatToShow="TRADES",
        useRTH=False,  # True: Regular trading hours only
        formatDate=2,

    )


    if os.path.exists(path_spot):
        updateCsvSpot(bars, path_spot)
    else:
        writeCsvSpot(bars, path_spot)

ib.disconnect()