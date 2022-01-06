import os
from datetime import datetime, timedelta
import yfinance as yf
import threading
from catalog_symbols import CatalogSymbols
from apscheduler.schedulers.background import BackgroundScheduler
from fastparquet_utils import convert_csvs_to_parquet

NUMBER_OF_PRICES_AVG = 10  # dynamic average


def ask_user_for_symbols(symbols_to_choose: {}) -> {}:
    while True:
        symbols = input(f"Please choose a symbol from the list to investigate {symbols_to_choose}: \n ").split(" ") #input of a symbol from the list
        symbols_set = set(symbols) #the symbol that's given from the user
        symbols_to_choose_set = set(symbols_to_choose) #the entire symbols list, removes duplicate
        if symbols_set.issubset(symbols_to_choose_set): #return True if all items in set x are present in set y
            break
    print(f"The chosen symbol is: {symbols}")
    return {symbol: symbols_to_choose.get(symbol) for symbol in symbols} # returns a dict with only the chosen symbols and its frequency


def save_symbols_frequency_to_csv(symbol, symbols_prices):
    file_name = f"{symbol}.csv"
    interval = f"{1}m"
    ticker = yf.Ticker(symbol) #defining the symbol from the library
    ticker_df = ticker.history(interval=interval, start=datetime.now() - timedelta(days=1), period="1d") #reading the symbol history
    if os.path.exists(file_name):
        write_mode = 'a' #append if already exists
        header = False   #ignore the header
    else:
        write_mode = 'w' #make a new file if not exists
        header = True    #add the header to the head of file
    ticker_df.to_csv(file_name, mode=write_mode, header=header) #write\append the ticker.history to the CSV file
    print(f"{file_name} file has been written")

    #add price to dict "symbols_prices", for example: {"AAPL": [339, 200]}
    price = ticker.info['currentPrice']
    symbols_prices.setdefault(symbol, []) #if symbol not exists on symbols_prices - create it and add [] to his value
    if len(symbols_prices[symbol]) == NUMBER_OF_PRICES_AVG:
        symbols_prices[symbol].pop() #remove the first item (is the oldest one)
    symbols_prices[symbol].append(price) #add the new item (is the newest one) (the original!!)
    print(f"{price} price")


if __name__ == '__main__':
    symbols_prices = {}  # {'aapl': [10,10]}
    symbols_threads = {} #define the threads for every symbol (dict for unknown size + symbol and its frequency)
    scheduler = BackgroundScheduler() #for parallel threads
    catalog_symbols = CatalogSymbols() #function of creating\reading\writing to the CSV file
    symbols_with_frequency_to_choose: {} = catalog_symbols.read_symbols() #reading the symbol and its frequency
    scheduler.add_job(convert_csvs_to_parquet, 'interval', minutes=2, args=[symbols_threads, symbols_prices]) #parallel symbols_threads for parquet file w price calculation
    scheduler.start() #start the parquet tread
    while True: #creating thread for symbol every "frequency" seconds to make a CSV file
        symbols_frequency_sec: {} = ask_user_for_symbols(symbols_with_frequency_to_choose) #ask a symbol and returns the chosen symbol & frequency
        for symbol, frequency in symbols_frequency_sec.items():     #creating thread for symbol every "frequency" seconds to make a CSV file
            if symbol not in symbols_threads:
                symbols_threads[symbol] = scheduler.add_job(
                    save_symbols_frequency_to_csv, 'interval', seconds=frequency, args=[symbol, symbols_prices])
