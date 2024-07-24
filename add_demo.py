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
        self.csv_directory = os.path.join('d:','original','2021-2023')
        self.datetime_fmt = '%m/%d/%Y %H:%M'
        self.date_fmt = '%m/%d/%Y'
        self.mrn_data = dict()
        self.mrn_count = dict()

    def count_rows_per_mrn(self):
        line_count = 0
        with open(os.path.join(self.csv_directory, 'results_with_followup.csv'), 'r') as f:
            line_count = 0
            reader = csv.reader(f)
            for row in reader:
                if line_count == 0:
                    # skip header line
                    line_count += 1
                else:
                    line_count += 1
                    mrn = row[0]
                    if mrn in self.mrn_count:
                        c = self.mrn_count[mrn]
                        self.mrn_count[mrn] = c + 1
                    else:
                        self.mrn_count[mrn] = 1

        print(f'Processed load_mrn_data {line_count} lines.')

    def load_mrn_data(self):
        line_count = 0
        with open(os.path.join(self.csv_directory, 'population_recoded.csv'), 'r') as f:
            line_count = 0
            reader = csv.reader(f)
            for row in reader:
                if line_count == 0:
                    # skip header line
                    line_count += 1
                else:
                    line_count += 1
                    mrn = row[0]
                    demo_list = list()
                    for i in range (2, 13):
                        demo_list.append(row[i])
                    self.mrn_data[mrn] = demo_list
        print(f'Processed load_mrn_data {line_count} lines.')

    def test_date_list(self, date_list, match_date):
        for d in date_list:
            delta = d - match_date
            days = delta.days
            if days > 0 and days <= 365:
                return ('Y', d)
        return ('N', datetime.date(1900, 1, 1))

    def add_demo(self):
        print("add_demo")
        line_count = 0
        with open(os.path.join(self.csv_directory, 'results_with_followup.csv'), 'r', encoding='utf-8') as f:
            with open (os.path.join(self.csv_directory, 'all_results.csv'), 'w', newline='', encoding='utf-8') as out:
                with open (os.path.join(self.csv_directory, 'aaa_black_ltfu_with_late.csv'), 'w', newline='', encoding='utf-8') as lfu:
                # with open (os.path.join(self.csv_directory, 'aaa_black_ltfu.csv'), 'w', newline='', encoding='utf-8') as lfu:
                    out_writer = csv.writer(out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    lfu_writer = csv.writer(lfu, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    header = ('mrn', 'encounter_id', 'cyto_result', 'hpvdna_result', 'hpv_other', 'hpv16', 'hpv18', 'screening_result', 'collection_date', 'dob', 'age', 'comments', 'colpo_fu', 'colpo_fu_date', 'colpo_late_fu', 'leep_fu', 'leep_fu_date', 'leep_late_fu', 'deceased_date', 'study_race', 'source_race', 'ethnicity', 'lastname', 'firstname', 'middlename', 'postalcode', 'homephone', 'mobilephone', 'email', 'mrn_count')
                    out_writer.writerow(header)
                    lfu_writer.writerow(header)
                    line_count = 0
                    reader = csv.reader(f)
                    early_date = datetime.date(1900, 1, 1)
                    for row in reader:
                        if line_count == 0:
                            # skip header line
                            line_count += 1
                        else:
                            line_count += 1
                            mrn = row[0]
                            if mrn in self.mrn_data:
                                demo_list = self.mrn_data[mrn]
                                for val in demo_list:
                                    row.append(val)
                                if mrn in self.mrn_count:
                                    row.append(self.mrn_count[mrn])
                                else:
                                    row.append('0')
                                out_writer.writerow(row)
                                # if 'Black' in row[19] and row[7] == 'High' and row[12] == 'N' and row[14] == 'N' and row[15] == 'N' and row[17] == 'N':
                                if 'Black' in row[19] and row[7] == 'High' and row[12] == 'N' and row[15] == 'N':
                                    lfu_writer.writerow(row)
                            else:
                                print(f'mrn {mrn} not in demographics')
            print(f'Processed {line_count} screened lines.')

    def main(self):
        print("main")
        print(f'csv_directory: {self.csv_directory}')
        self.load_mrn_data()
        self.count_rows_per_mrn()
        self.add_demo()

if __name__ == '__main__':
    filter = Filter()
    filter.main()
