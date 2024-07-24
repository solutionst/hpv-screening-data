# c. 2024 solutionst.com llc
# This code is licensed under the MIT license (See LICENSE for details).
import csv
import os
import sys
import datetime

class Filter:
    def __init__(self):
        print("__init__")
        self.script_directory = os.path.dirname(os.path.abspath(sys.argv[0]))
        self.csv_directory = os.path.join(self.script_directory, 'csv')
        self.date_format = '%m/%d/%Y %H:%M'
        self.mrn_black = dict()
        self.leep_dates = dict()
        self.colpo_dates = dict()

    def process_screened_women(self):
        print("process_screened_women")
        line_count = 0
        with open(os.path.join(self.csv_directory, 'abnormal screens requiring follow up about 962.csv'), 'r', encoding='utf-8') as f:
            with open (os.path.join(self.csv_directory, 'black_screened_women_3.csv'), 'w', newline='', encoding='utf-8') as out:
                out_writer = csv.writer(out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                line_count = 0
                reader = csv.reader(f)
                for row in reader:
                    if line_count == 0:
                        # skip header line
                        row.append('postalcode')
                        row.append('leep_min')
                        row.append('leep_max')
                        row.append('colpo_min')
                        row.append('colpo_max')
                        out_writer.writerow(row)
                        line_count += 1
                    else:
                        line_count += 1
                        mrn = row[0]
                        if mrn in self.mrn_black:
                            postal_code = self.mrn_black[mrn]
                            row.append(postal_code)
                            if mrn in self.leep_dates:
                                tuple = self.leep_dates[mrn]
                                leep_min = tuple[0]
                                leep_max = tuple[1]
                                leep_min_str = datetime.datetime.strftime(leep_min, self.date_format)
                                row.append(leep_min_str)
                                row.append(datetime.datetime.strftime(leep_max, self.date_format))
                            else:
                                row.append(None)
                                row.append(None)
                            if mrn in self.colpo_dates:
                                tuple = self.colpo_dates[mrn]
                                colpo_min = tuple[0]
                                colpo_max = tuple[1]
                                colpo_min_str = datetime.datetime.strftime(colpo_min, self.date_format)
                                row.append(colpo_min_str)
                                row.append(datetime.datetime.strftime(colpo_max, self.date_format))
                            else:
                                row.append(None)
                                row.append(None)
                            out_writer.writerow(row)
            print(f'Processed {line_count} screened lines.')


    def load_black_mrn(self):
        line_count = 0
        with open(os.path.join(self.csv_directory, 'NEW x 2 BLACK numbers to sort.csv'), 'r') as f:
            line_count = 0
            reader = csv.reader(f)
            for row in reader:
                if line_count == 0:
                    # skip header line
                    line_count += 1
                else:
                    line_count += 1
                    # print(f'row[1]: {row[1]}')
                    # self.mrn_black.add(row[1])
                    mrn = row[2]
                    postal = row[19]
                    self.mrn_black[mrn] = postal
        print(f'Processed load_black_mrn {line_count} lines.')

    def load_leep_dates(self):
        line_count = 0
        with open(os.path.join(self.csv_directory, 'hpi10029_LEEP_Labresults_2021_to_2023.csv'), 'r') as f:
            line_count = 0
            reader = csv.reader(f)
            for row in reader:
                if line_count == 0:
                    # skip header line
                    line_count += 1
                else:
                    line_count += 1
                    mrn = row[0]
                    date_str = row[5]
                    the_date = datetime.datetime.strptime(date_str, self.date_format)
                    if mrn in self.leep_dates:
                        tuple = self.leep_dates[mrn]
                        # tuple contains min and max collection dates for mrn
                        min_date = tuple[0]
                        max_date = tuple[1]
                        if the_date < min_date:
                            min_date = the_date
                        if the_date > max_date:
                            max_date = the_date
                        self.leep_dates[mrn] = (min_date, max_date)
                    else:
                        self.leep_dates[mrn] = (the_date, the_date)
        print(f'Processed load_leep_dates {line_count} lines.')

    def load_colpo_dates(self):
        line_count = 0
        with open(os.path.join(self.csv_directory, 'hpi10029_colpo_labresults_2021_to_2023.csv'), 'r') as f:
            line_count = 0
            reader = csv.reader(f)
            for row in reader:
                if line_count == 0:
                    # skip header line
                    line_count += 1
                else:
                    line_count += 1
                    mrn = row[0]
                    date_str = row[5]
                    the_date = datetime.datetime.strptime(date_str, self.date_format)
                    if mrn in self.colpo_dates:
                        tuple = self.colpo_dates[mrn]
                        # tuple contains min and max collection dates for mrn
                        min_date = tuple[0]
                        max_date = tuple[1]
                        if the_date < min_date:
                            min_date = the_date
                        if the_date > max_date:
                            max_date = the_date
                        self.colpo_dates[mrn] = (min_date, max_date)
                    else:
                        self.colpo_dates[mrn] = (the_date, the_date)
        print(f'Processed load_colpo_dates {line_count} lines.')

    def main(self):
        print("main")
        print(f'csv_directory: {self.csv_directory}')
        self.load_black_mrn()
        self.load_leep_dates()
        self.load_colpo_dates()
        self.process_screened_women()

if __name__ == '__main__':
    filter = Filter()
    filter.main()
