import os
from datetime import datetime
import csv
import glob
from concurrent.futures import ThreadPoolExecutor

# Returns true or false based on if b is unrealistic based on an assumption
def verify_magnitude(a, b):
    if b >= a * 5:
        return True
    elif b <= a / 5:
        return True
    elif b < 100:
        return True
    else:
        return False

# iterates through each row of a .csv file
def read_rows(path):
    with open(path, "r") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            yield row

class DataPreprocessor:
    def __init__(self, input_folder, output_folder, num_threads):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.num_threads = num_threads
        os.makedirs(self.output_folder, exist_ok=True)

        print(f"Parsed Parameters:\n- Original Data: {self.input_folder}\n- Cleaned Data Path: {self.output_folder}\n"
              f"- Thread count: {self.num_threads}")

        print("Please wait while data is cleaned.")

    # Validates and cleans a single .csv file and returns a list of lists
    def validate_and_clean_file(self, file_path):
        cleaned_data = []
        unique_timestamps = set()
        gen = read_rows(file_path)
        last_valid_price = -1.0

        # Validate and clean each row
        for row in gen:
            if row[0] == "" or row[1] == "" or row[2] == "":
                continue # Ignore ticks with missing values

            try:
                formatted_row = [datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f"), float(row[1]), int(row[2])]
                if last_valid_price == -1.0:
                    last_valid_price = formatted_row[1]

                if formatted_row[0] in unique_timestamps:
                    continue # Ignores ticks with duplicate timestamps

                if formatted_row[1] < 0:
                    formatted_row[1] = abs(formatted_row[1]) # Corrects for negative values

                if verify_magnitude(last_valid_price, formatted_row[1]):
                    continue # Ignores values with unrealistic magnitudes

                if formatted_row[2] < 0:
                    continue #Ignores ticks with negative size

                cleaned_data.append(formatted_row)
            except ValueError:
                pass # Ignores ticks with missing timestamps, prices, or sizes

        return cleaned_data

    # Saves cleaned data to a new .csv file

    def save_cleaned_data(self, cleaned_data, output_file):
        with open(output_file, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Timestamp", "Price", "Size"])
           #  print(cleaned_data)
            writer.writerows(cleaned_data)

    # Processes a single .csv file: cleans it and saves the result.
    def process_single_file(self, file_path):
        cleaned_data = self.validate_and_clean_file(file_path)
        output_file = os.path.join(self.output_folder, os.path.basename(file_path))
        self.save_cleaned_data(cleaned_data, output_file)

    # Uses multithreading to process all files in the folder.
    def process_all_files(self):

        file_paths = glob.glob(os.path.join(self.input_folder, "*.csv"))

        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            executor.map(self.process_single_file, file_paths)

        print("Data cleaning complete!")

                