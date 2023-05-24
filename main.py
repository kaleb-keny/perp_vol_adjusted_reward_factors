import ccxt
from web3 import Web3
import pandas as pd
import numpy as np

w3       = Web3(Web3.HTTPProvider('https://opt-mainnet.g.alchemy.com/v2/XXXXXXXXXXXXXXXXX'))
exchange = ccxt.binance()
ADDRESS_RESOLVER_ADDRESS = '0x95A6a3f44a70172E7d50a9e28c85Dfd712756B8C'
ADDRESS_RESOLVER_ABI     = '''[{ "constant": true, "inputs": [ { "internalType": "bytes32", "name": "name", "type": "bytes32" } ], "name": "getAddress", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "payable": false, "stateMutability": "view", "type": "function" }]'''

def get_realized_vol(ticker):
    if ticker.lower() in ['xau','xag']:
        ticker = 'paxg'
    symbol  = ticker.upper() + "/USDT"
    output  = exchange.fetch_ohlcv(symbol,"1h",limit=1000)
    df      = pd.DataFrame(output,columns=["timestamp","o","h","l","c","v"])
    returns = df["c"].apply(np.log).diff(1).dropna()
    return np.sqrt(np.sum(np.square(returns)))

contract = w3.eth.contract(address=ADDRESS_RESOLVER_ADDRESS ,abi=ADDRESS_RESOLVER_ABI)
marketDataAddress  = contract.functions.getAddress(w3.toHex(text='PerpsV2MarketData')).call()
MARKET_DATA_ABI    = '''[{"constant":true,"inputs":[],"name":"allProxiedMarketSummaries","outputs":[{"components":[{"internalType":"address","name":"market","type":"address"},{"internalType":"bytes32","name":"asset","type":"bytes32"},{"internalType":"bytes32","name":"key","type":"bytes32"},{"internalType":"uint256","name":"maxLeverage","type":"uint256"},{"internalType":"uint256","name":"price","type":"uint256"},{"internalType":"uint256","name":"marketSize","type":"uint256"},{"internalType":"int256","name":"marketSkew","type":"int256"},{"internalType":"uint256","name":"marketDebt","type":"uint256"},{"internalType":"int256","name":"currentFundingRate","type":"int256"},{"internalType":"int256","name":"currentFundingVelocity","type":"int256"},{"components":[{"internalType":"uint256","name":"takerFee","type":"uint256"},{"internalType":"uint256","name":"makerFee","type":"uint256"},{"internalType":"uint256","name":"takerFeeDelayedOrder","type":"uint256"},{"internalType":"uint256","name":"makerFeeDelayedOrder","type":"uint256"},{"internalType":"uint256","name":"takerFeeOffchainDelayedOrder","type":"uint256"},{"internalType":"uint256","name":"makerFeeOffchainDelayedOrder","type":"uint256"}],"internalType":"struct PerpsV2MarketData.FeeRates","name":"feeRates","type":"tuple"}],"internalType":"struct PerpsV2MarketData.MarketSummary[]","name":"","type":"tuple[]"}],"payable":false,"stateMutability":"view","type":"function"}]'''
marketDataContract = w3.eth.contract(address=marketDataAddress,abi=MARKET_DATA_ABI) 
summaries          = marketDataContract.functions.allProxiedMarketSummaries().call()
tickerList         = [w3.toText(summary[2])[1:].replace("\x00","").replace("PERP","") for summary in summaries]
volDict            = {ticker: get_realized_vol(ticker) for ticker in tickerList}

output = list()

for ticker, vol in volDict.items():
    output.append([ticker,round(vol/volDict["ETH"],5)])
    
df = pd.DataFrame(output,columns=["ticker","VolRank"])
df.sort_values(by=["VolRank"],ascending=False,inplace=True)
print(df)
df.to_csv("output.csv",index=False)
