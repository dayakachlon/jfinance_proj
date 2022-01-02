import yfinance as yf
import threading
from catalog_symbols import CatalogSymbols
from apscheduler.schedulers.background import BackgroundScheduler
from fastparquet_utils import convert_csvs_to_parquet


def ask_user_for_symbols(symbols_to_choose: {}) -> {}:
    while True:
        symbols = input(f"Please choose symbols from the list to investigate {symbols_to_choose}: \n ").split(" ") #input of a symbol from the list
        symbols_set = set(symbols) #the symbol that's given from the user
        symbols_to_choose_set = set(symbols_to_choose) # the entire symbols lists
        if symbols_set.issubset(symbols_to_choose_set): #checks if the user's choose is from the available list (yes-break, no-ask for new)
            break
    print(f"The chosen symbols is: {symbols}")
    return {symbol: symbols_to_choose.get(symbol) for symbol in symbols} #returns the chosen symbol and its frequency


def save_symbols_frequency_to_csv(symbol):
    interval = f"{1}m"
    ticker = yf.Ticker(symbol)
    ticker_df = ticker.history(interval=interval, period="1d")
    data_frame = ticker_df.head()
    data_frame.to_csv(f"{symbol}.csv", mode='a')


if __name__ == '__main__':
    symbols_threads = {} #define the threads for every symbol (dict for unknown size + symbol and its frequency)
    scheduler = BackgroundScheduler() #for parallel threads
    catalog_symbols = CatalogSymbols() #function of creating\reading\writing to the CSV file
    symbols_with_frequency_to_choose: {} = catalog_symbols.read_symbols() #reading the symbol and its frequency
    scheduler.add_job(convert_csvs_to_parquet, 'interval', minutes=2, args=[symbols_threads]) #adding parallely threads from chosen symbols to make parquet file
    scheduler.start() #start the parquet tread
    while True: #creating thread for symbol every "frequency" minutes
        symbols_frequency_sec: {} = ask_user_for_symbols(symbols_with_frequency_to_choose) #ask a symbol and returns the chosen symbol & frequency
        for symbol, frequency in symbols_frequency_sec.items(): #checks from the chosen symbols with their frequency
            if symbol not in symbols_threads: #if there is already an existing thread for that symbol (if not-continue)
                symbols_threads[symbol] = threading.Timer(  #making threads for (key:value) into the dict - the chosen symbols
                    interval=frequency,                     #the interval (freq) to go to the function
                    function=save_symbols_frequency_to_csv, #the desire function -creating CSV file for any symbol
                    args=[symbol])                          #sends the symbol as an argument for the function
                try:
                    symbols_threads[symbol].join()  # if the thread runs -continue
                except Exception:
                    symbols_threads[symbol].start() # if the thread did not run -start the thread


