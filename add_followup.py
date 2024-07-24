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
        self.leep_dates = dict()
        self.colpo_dates = dict()

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
                    the_date = datetime.datetime.strptime(date_str, self.datetime_fmt).date()
                    if mrn in self.leep_dates:
                        date_list = self.leep_dates[mrn]
                        # list contains colpo dates for mrn
                        if the_date not in date_list:
                            date_list.append(the_date)
                    else:
                        the_list = list()
                        the_list.append(the_date)
                        self.leep_dates[mrn] = the_list
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
                    the_date = datetime.datetime.strptime(date_str, self.datetime_fmt).date()
                    if mrn in self.colpo_dates:
                        date_list = self.colpo_dates[mrn]
                        # list contains colpo dates for mrn
                        if the_date not in date_list:
                            date_list.append(the_date)
                    else:
                        the_list = list()
                        the_list.append(the_date)
                        self.colpo_dates[mrn] = the_list
        print(f'Processed load_colpo_dates {line_count} lines.')

    def test_date_list(self, date_list, match_date):
        late_fu = 'N'
        for d in date_list:
            delta = d - match_date
            days = delta.days
            if days >= 0 and days <= 365:
                return ('Y', d, late_fu)
            elif days >= 1:
                late_fu = 'Y'
        return ('N', datetime.date(1900, 1, 1), late_fu)

    def add_followup(self):
        print("add_followup")
        line_count = 0
        with open(os.path.join(self.csv_directory, 'results_2021_collapsed.csv'), 'r', encoding='utf-8') as f:
            with open (os.path.join(self.csv_directory, 'results_with_followup.csv'), 'w', newline='', encoding='utf-8') as out:
                out_writer = csv.writer(out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                header = ('mrn', 'encounter_id', 'cyto_result', 'hpvdna_result', 'hpv_other', 'hpv16', 'hpv18', 'screening_result', 'collection_date', 'dob', 'age', 'comments', 'colpo_fu', 'colpo_fu_date', 'colpo_late_fu', 'leep_fu', 'leep_fu_date', 'leep_late_fu')
                out_writer.writerow(header)
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
                        # lookup dates
                        date_str = row[8]
                        collection_date = datetime.datetime.strptime(date_str, self.date_fmt).date()
                        colpo_fu = 'N'
                        colpo_fu_date = early_date
                        colpo_late_fu = 'N'
                        if mrn in self.colpo_dates:
                            date_list = self.colpo_dates[mrn]
                            colpo_tuple = self.test_date_list(date_list, collection_date)
                            colpo_fu = colpo_tuple[0]
                            colpo_fu_date = colpo_tuple[1]
                            colpo_late_fu = colpo_tuple[2]
                        leep_fu = 'N'
                        leep_fu_date = early_date
                        leep_late_fu = 'N'
                        if mrn in self.leep_dates:
                            date_list = self.leep_dates[mrn]
                            leep_tuple = self.test_date_list(date_list, collection_date)
                            leep_fu = leep_tuple[0]
                            leep_fu_date = leep_tuple[1]
                            leep_late_fu = leep_tuple[2]
                        row.append(colpo_fu)
                        row.append(colpo_fu_date)
                        row.append(colpo_late_fu)
                        row.append(leep_fu)
                        row.append(leep_fu_date)
                        row.append(leep_late_fu)
                        out_writer.writerow(row)
            print(f'Processed {line_count} screened lines.')

    def main(self):
        print("main")
        print(f'csv_directory: {self.csv_directory}')
        self.load_colpo_dates()
        self.load_leep_dates()
        self.add_followup()

if __name__ == '__main__':
    filter = Filter()
    filter.main()
