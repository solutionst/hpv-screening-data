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
        self.csv_directory = os.path.join('e:','original','2021-2023')
        self.date_format = '%m/%d/%Y %H:%M'
        self.cyto_dict = dict()
        self.tgyns_dict = dict()
        self.hpvdna_dict = dict()
    
    def write_dictionaries(self):
        with open (os.path.join(self.csv_directory, 'work_dictionary.csv'), 'w', newline='', encoding='utf-8') as out:
            out_writer = csv.writer(out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            header = ('order_code', 'order_name', 'result_code', 'result_name', 'value')
            out_writer.writerow(header)
            for key in self.tgyns_dict:
                out_writer.writerow(self.tgyns_dict[key])
            for key in self.hpvdna_dict:
                out_writer.writerow(self.hpvdna_dict[key])
            for key in self.cyto_dict:
                out_writer.writerow(self.cyto_dict[key])
            

    def make_dictionaries(self):
        print("make_cyto_dict")
        line_count = 0
        with open(os.path.join(self.csv_directory, 'hpi10029_cervical_and_Cyto_labresults_2021_to_2023.csv'), 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if line_count == 0:
                    # skip header line
                    line_count += 1
                else:
                    line_count += 1
                    order_code = row[8]
                    order_name = row[9]
                    result_code = row[10]
                    result_name = row[11]
                    value = row[12]
                    if order_code != 'SPECIMEN' and result_code not in ['CLINF', 'ADEQ', 'GROSS', 'COMMENT', 'COMMENTS']:
                        if not (value.startswith(' http://') or value.startswith(' https://') or value == ' DNR'):
                            tuple = (order_code, order_name, result_code, result_name, value)
                            key = "#".join(tuple)
                            match order_code:
                                case 'TGYNS' | 'TDGYNS' | 'CYTONG':
                                    if key not in self.tgyns_dict:
                                        self.tgyns_dict[key] = tuple
                                case 'HPVDNA':
                                    if key not in self.hpvdna_dict:
                                        self.hpvdna_dict[key] = tuple
                                case _:
                                    if key not in self.cyto_dict:
                                        self.cyto_dict[key] = tuple
            print(f'Processed {line_count} screened lines.')    

    def main(self):
        print("main")
        print(f'csv_directory: {self.csv_directory}')
        self.make_dictionaries()
        self.write_dictionaries()

if __name__ == '__main__':
    filter = Filter()
    filter.main()
