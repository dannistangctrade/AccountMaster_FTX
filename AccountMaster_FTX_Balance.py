
import ccxt
import os
import psycopg2
import pandas as pd
from datetime import datetime



#contract to ftx prep
ftx_contract = {
    "BTC": "BTC-PERP",
    "ETH": "ETH-PERP"
}

position_dict = {
    "BTC": "BTCUSD",
    "ETH": "ETHUSD"
}

# configure exchange
exchange = getattr(ccxt, 'ftx')({
  'apiKey': '',
  'secret': '',
  'timeout': 10000,
  'enableRateLimit': True,
  'headers': { 'FTX-SUBACCOUNT': 'hedge' }
})

conn = psycopg2.connect(
    host="",
    database="",
    user="",
    password="")


cur = conn.cursor()

##wallet balance details of customers
cur.execute(f'select "Currency", sum("Balance") from (select row_number() over (PARTITION BY "CID", "Currency" order by "DateTime" desc) as "num", * from "tbl_AccountMasters") account where "num" = 1 and "CID" not in (\'250250\', \'250251\', \'250252\',\'38AA56DF581093\', \'38B12F19EA082D\', \'38B0C6BF2650EA\', \'38B0984E7E9B72\', \'38B09714E4930A\', \'38B098A3C2F4A6\', \'3911E3C0DD7E72\', \'38AA47CD97E7B4\', \'38AA57AD9E8C2A\', \'38B0C6A5FACB5A\', \'38AA683E57733B\', \'38AA49633F80E0\', \'38B1269D203205\', \'38AB0D58CCD5E6\') and "CID" not in (select "UserID" from "public"."tbl_Customers" where "Status" = 2) and "Balance" > 0 group by "Currency"')
rows = cur.fetchall()
# crypto_list = [row[0] for row in rows]
balance = exchange.fetch_balance()
balance = balance.get('info')
balance_result = balance.get('result')

##get positions
cur.execute(f'select "symbol", sum("size") from "public"."tbl_Positions" where "closed" is false and "user_id" not in (\'38AA56DF581093\', \'38B12F19EA082D\', \'38B0C6BF2650EA\', \'38B0984E7E9B72\', \'38B09714E4930A\', \'38B098A3C2F4A6\', \'3911E3C0DD7E72\', \'250250\', \'250251\', \'250252\') and "user_id" not in (select "UserID" from "public"."tbl_Customers" where "Status" = 2) group by "symbol"')
positions = cur.fetchall()

def cal_upnl(contract, mark_price):
    cur.execute(f'select "avg_entry_price", "size" from "public"."tbl_Positions" where "closed" is false and "user_id" not in (\'38AA56DF581093\', \'38B12F19EA082D\', \'38B0C6BF2650EA\', \'38B0984E7E9B72\', \'38B09714E4930A\', \'38B098A3C2F4A6\', \'3911E3C0DD7E72\', \'250250\', \'250251\', \'250252\') and "user_id" not in (select "UserID" from "public"."tbl_Customers" where "Status" = 2) and "symbol" = \'{contract}\'')
    pnl_positions = cur.fetchall()
    position_list = [x[1]*(1/float(x[0]) - 1/mark_price) for x in pnl_positions]
    upnl = sum(position_list)
    return upnl




def ftx_info(contract):
    ##position from ftx
    positions = exchange.fetch_positions()
    for position in positions:
        if position.get('future') == ftx_contract[contract]:
            if position.get('side') == 'sell':
                hedge_size = float(position['size']) * -1
            else:
                hedge_size = float(position['size'])
    ##market price from ftx
    ticker = ftx_contract[contract]
    ticker_info = exchange.fetch_ticker(ticker)
    market_price = float(ticker_info['info']['price'])
    return hedge_size, market_price


result = []
for row in rows:
    sub_result = []
    for x in balance_result:
        if x['coin'] == row[0]:
            hedge_size, market_price = ftx_info(row[0])
            upnl = cal_upnl(position_dict[row[0]],market_price)
            ##crypto type
            sub_result.append(row[0])
            ##mark price
            sub_result.append(market_price)
            ##accountMaster
            sub_result.append(row[1])
            ##ftx balance
            sub_result.append(x['total'])
            ##upnl
            sub_result.append(upnl)
            ##position size
            for position in positions:
                if position_dict[row[0]] == position[0]:
                    sub_result.append(position[1]/market_price)
            ##position hedge
            sub_result.append(hedge_size)
    result.append(sub_result)
##usd
usd = []
for x in balance_result:
    if x['coin'] == 'USD':
        usd.append('USD')
        usd.append(1)
        usd.append(None)
        usd.append(x['total'])
        ##upnl
        usd.append(None)
        ##poistion size
        usd.append(None)
        ## position hedge
        usd.append(None)
        break
result.append(usd)

df = pd.DataFrame(result, columns=['Crypto', 'Mark_Price', 'AccountMaster_Balance', 'FTX_Balance', 'Unrealised Pnl', 'Position Size','Position Hedge'])
print(df)
##customise path to save the csv file
now = datetime.now()
##customize files dir
path = f'/Users/dannis.tang/PycharmProjects/AccountMaster_FTX/{now}'
os.mkdir(path)
df.to_csv(f'{now}/AccountMaster_FTX_Balance.csv')
print(f'AccountMaster_FTX_Balance.csv')
#########################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################


cur.execute(f'select "CID","Currency", "Balance" from (select row_number() over (PARTITION BY "CID", "Currency" order by "DateTime" desc) as "num", * from "tbl_AccountMasters") account where "num" = 1 and "CID" not in (\'250250\', \'250251\', \'250252\',\'38AA56DF581093\', \'38B12F19EA082D\', \'38B0C6BF2650EA\', \'38B0984E7E9B72\', \'38B09714E4930A\', \'38B098A3C2F4A6\', \'3911E3C0DD7E72\', \'38AA47CD97E7B4\', \'38AA57AD9E8C2A\', \'38B0C6A5FACB5A\', \'38AA683E57733B\', \'38AA49633F80E0\', \'38B1269D203205\', \'38AB0D58CCD5E6\')  and "CID" not in (select "UserID" from "public"."tbl_Customers" where "Status" = 2) and "Currency"=\'BTC\' order by "Balance" desc')
user_btc_balance = cur.fetchall()
user_btc_balance_list = []
for x in user_btc_balance:
    user_btc_balance_list.append(list(x))
user_btc_balance_list_df = pd.DataFrame(user_btc_balance_list, columns=['UserID', 'Crypto', 'Balance'])
print(user_btc_balance_list_df)
user_btc_balance_list_df.to_csv(f'{now}/Customer_BTC_Balance.csv')
print(f'Customer_BTC_Balance.csv')


# # load markets and all coin_pairs
# exchange.load_markets()
# positions = exchange.fetch_positions()
# for position in positions:
#   if position.get('future') == 'BTC-PERP':
#     print(position)
#
# # trades = exchange.fetch_my_trades
# # print(trades)
# openorder = exchange.fetch_open_orders()
# print(openorder)
#
#
# balance = exchange.fetch_balance()
# print(f'balance:  {balance}')
#
#mark price
# ticker = 'BTC-PERP'
# price = exchange.fetch_ticker(ticker)
# info = price['info']['price']
# print(type(info))
# positions = exchange.fetch_positions()
# for position in positions:
#     if position.get('future') == ftx_contract[contract]:
#         hedge_size = float(position['size'])


