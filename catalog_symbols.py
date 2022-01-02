import csv
import os


class CatalogSymbols:
    def __init__(self): #creating a CSV file
        self.csv_file = "csv_catalog_symbols.csv"

    def init_symbols(self): #filling the CSV file with data
        row_list = [
            ["symbol name", "Frequency"],
            ["AAPL", 5],
            ["MSFT", 5]
        ]
        with open(self.csv_file, 'w', newline='') as file: #Return a writer object responsible for converting the user’s data into delimited strings on the given file-like object.
            writer = csv.writer(file)
            writer.writerows(row_list)
            file.close()

    def read_symbols(self): #if there is no file - create + reading the symbol and its frequency
        if not os.path.isfile(self.csv_file):
            self.init_symbols()
        with open(self.csv_file, mode='r') as csv_file:
            file_data = csv.reader(csv_file)
            headers = next(file_data)
            d = {row[0]: int(row[1]) for row in file_data}
            csv_file.close()
            return d
