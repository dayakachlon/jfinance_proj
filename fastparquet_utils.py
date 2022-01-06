import pandas as pd


def convert_csvs_to_parquet(symbols_threads, symbols_prices):
    for symbol in symbols_threads:
        symbols_prices.setdefault(symbol, []) #if symbol not exists on symbols_prices - create it and add [] to his value
        prices = symbols_prices.get(symbol)   #saving the prices of the symbol
        avg_price = 0
        len_of_prices = len(prices)
        for price in prices:                  #calculate the sum of all prices
            avg_price += price
        avg_price = avg_price / len_of_prices
        print(f"symbol: {symbol}: , the avg price for {len_of_prices} last prices are: {avg_price}")
        data_frame = pd.read_csv(f"{symbol}.csv") #reading the CSV file

        #add the avg_price to the exists data from the csv
        data_for_parquet = data_frame.append(pd.DataFrame({'avg_price': [avg_price]}))
        data_for_parquet.to_parquet(f"{symbol}.parquet") #creating the parquet file for every symbol
