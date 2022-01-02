import pandas as pd


def convert_csvs_to_parquet(symbols_threads):
    print(symbols_threads)
    for symbol in symbols_threads:
        data_frame = pd.read_csv(f"{symbol}.csv")
        print(data_frame)
        data_frame.to_parquet(f"{symbol}.parquet")
