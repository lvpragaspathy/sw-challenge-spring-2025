import csv
import datetime
import os
import glob
import re
import bisect
from concurrent import futures


class OHLCVGenerator:
    def __init__(self, start_time, end_time, interval, data_path):
        self.start_time = self.parse_datetime(start_time)
        self.end_time = self.parse_datetime(end_time)
        self.interval = self.parse_interval(interval)
        self.data_path = data_path


        print(f"Parsed Parameters:\n- Start: {self.start_time}\n- End: {self.end_time}\n"
              f"- Interval: {self.interval}")

        if self.start_time >= self.end_time:
            raise ValueError("Start time must be before end time")




    @staticmethod
    def parse_interval(interval_str):
        pattern = re.compile(r'(\d+)([dhms])', re.IGNORECASE)
        parts = pattern.findall(interval_str)
        if not parts:
            raise ValueError(f"Invalid interval format: {interval_str}")

        total_seconds = 0
        for value, unit in parts:
            value = int(value)
            unit = unit.lower()
            conversions = {
                'd': 86400,
                'h': 3600,
                'm': 60,
                's': 1
            }
            total_seconds += value * conversions[unit]

        if total_seconds <= 0:
            raise ValueError("Interval must be a positive duration")
        return datetime.timedelta(seconds=total_seconds)

    @staticmethod
    def parse_datetime(dt_str):
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f"
        ]
        for fmt in formats:
            try:
                return datetime.datetime.strptime(dt_str, fmt)
            except ValueError:
                continue
        raise ValueError(f"Invalid datetime format: {dt_str}")
    #Identify all relevant CSV files in the date range
    def get_relevant_files(self):
        files = []
        current_date = self.start_time.date()
        end_date = self.end_time.date()

        print(f"\nSearching dates from {current_date} to {end_date}")

        while current_date <= end_date:
            date_str = current_date.strftime("%Y%m%d")
            day_start = datetime.datetime.combine(current_date, datetime.time(9, 30))
            day_end = (datetime.datetime.combine(current_date, datetime.time(20, 59)) +
                       datetime.timedelta(minutes=1))

            effective_start = max(self.start_time, day_start)
            effective_end = min(self.end_time, day_end)

            if effective_start >= effective_end:
                current_date += datetime.timedelta(days=1)
                continue

            start_minute = max((effective_start.hour - 9) * 60 + (effective_start.minute - 29), 1)
            end_minute = min((effective_end.hour - 9) * 60 + (effective_end.minute - 29), 690)

            for minute_code in range(start_minute, end_minute + 1):
                pattern = os.path.join(self.data_path, f"ctg_tick_{date_str}_{minute_code:04d}_*.csv")
                files.extend(glob.glob(pattern))

            current_date += datetime.timedelta(days=1)
        return files

    #Read and process tick data files using thread pool
    def read_files_concurrently(self, files):
        all_ticks = []
        with futures.ThreadPoolExecutor() as executor:
            future_to_file = {
                executor.submit(self.process_single_file, file): file
                for file in files
            }
            for future in futures.as_completed(future_to_file):
                try:
                    all_ticks.extend(future.result())
                except Exception as e:
                    print(f"Error processing file: {e}")
        return sorted(all_ticks, key=lambda x: x[0])

    #Process individual CSV file
    def process_single_file(self, filename):
        ticks = []
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                if len(row) < 3:
                    continue
                try:
                    ts = self.parse_datetime(row[0])
                    if self.start_time <= ts <= self.end_time:
                        ticks.append((
                            ts,
                            float(row[1]),
                            int(row[2])
                        ))
                except (ValueError, IndexError):
                    continue
        return ticks

    # process data and write to CSV file
    def generate_csv(self, output_filename):

        files = self.get_relevant_files()
        if not files:
            raise ValueError(f"No .csv files found in specified range.")

        sorted_ticks = self.read_files_concurrently(files)

        if not sorted_ticks:
            raise ValueError("No tick data found in specified range")

        timestamps = [t[0] for t in sorted_ticks]
        prices = [t[1] for t in sorted_ticks]
        volumes = [t[2] for t in sorted_ticks]

        with open(output_filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Start', 'End', 'Open', 'High', 'Low', 'Close', 'Volume'])

            current_start = self.start_time
            while current_start < self.end_time:
                current_end = current_start + self.interval
                if current_end > self.end_time:
                    current_end = self.end_time

                # Find indices using binary search
                left = bisect.bisect_left(timestamps, current_start)
                right = bisect.bisect_left(timestamps, current_end)

                if left < right:
                    interval_prices = prices[left:right]
                    interval_volumes = volumes[left:right]

                    writer.writerow([
                        current_start.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        current_end.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        interval_prices[0],
                        max(interval_prices),
                        min(interval_prices),
                        interval_prices[-1],
                        sum(interval_volumes)
                    ])

                current_start = current_end
        print("OHLCV CSV generated at: ", output_filename)



