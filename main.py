import multiprocessing as mp
from data_preprocessing import DataPreprocessor
from ohlcv_generator import OHLCVGenerator

# Simple interface for this program.
def main():
    num_cores = mp.cpu_count()
    original_data_path = input("Enter file path for dataset: ")
    cleaned_data_path = input("Enter file path for cleaned data to be stored: ")

    cleaner = DataPreprocessor(original_data_path, cleaned_data_path, num_cores * 2)
    cleaner.process_all_files()

    while True:
        try:
            start_date = input("Enter start datetime (YYYY-MM-DD HH:MM:SS.SSS): ")
            end_date = input("Enter end datetime (YYYY-MM-DD HH:MM:SS.SSS): ")
            interval = input("Enter time interval (e.g., \'4s\', \'15m\', \'2h\', \'1d\', \'1h30m\'): ")
            output_name = input("Enter output file name (e.g., \'results.csv\'): ")
            generator = OHLCVGenerator(start_date, end_date, interval, cleaned_data_path)
            generator.generate_csv(output_name)
        except ValueError:
            print("Invalid time datetime or interval. Follow the valid format.")
            continue

        yes_or_no = input("Would you like to generate another OHLCV? [Y/N]: ")
        if yes_or_no.upper()  != 'Y':
            break

if __name__ == '__main__':
    main()


