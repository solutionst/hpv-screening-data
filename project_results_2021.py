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
        self.datetime_fmt = '%m/%d/%Y %H:%M'
        self.date_fmt = '%m/%d/%Y'
    
    def project_results(self):
        print("project_results")
        line_count = 0
        with open(os.path.join(self.csv_directory, 'hpi10029_cervical_and_Cyto_labresults_2021_to_2023.csv'), 'r', encoding='utf-8') as f:
            with open (os.path.join(self.csv_directory, 'results_2021.csv'), 'w', newline='', encoding='utf-8') as out:
                out_writer = csv.writer(out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                # mrn,PatientID,EncounterID,ACCESSION_NUMBER,ORDER_DATE,COLLECTION_DATE,OBSERVATION_DATE,ActivityDate,ORDER_CODE,ORDER_NAME,RESULT_CODE,RESULT_NAME,VALUE,RANGE,RESULT_COMMENT
                header = ('mrn', 'EncounterID', 'COLLECTION_DATE', 'ORDER_CODE', 'ORDER_NAME', 'RESULT_CODE' ,'RESULT_NAME', 'VALUE')
                out_writer.writerow(header)
                line_count = 0
                reader = csv.reader(f)
                for row in reader:
                    if line_count == 0:
                        # skip header line
                        line_count += 1
                    else:
                        line_count += 1
                        mrn = row[0]
                        encounter_id = row[2]
                        date_str = row[5]
                        collection_date = datetime.datetime.strptime(date_str, self.datetime_fmt)
                        order_code = row[8]
                        order_name = row[9]
                        result_code = row[10]
                        result_name = row[11]
                        value = row[12]
                        if order_code != 'SPECIMEN' and result_code not in ['CLINF', 'ADEQ', 'GROSS', 'COMMENT', 'COMMENTS']:
                            if not (value.startswith(' http://') or value.startswith(' https://') or value == ' DNR'):
                                collection_out = datetime.date.strftime(collection_date, self.date_fmt)
                                tuple = (mrn, encounter_id, collection_out, order_code, order_name, result_code, result_name, value)
                                out_writer.writerow(tuple)
            print(f'Processed {line_count} screened lines.')    

    def main(self):
        print("main")
        print(f'csv_directory: {self.csv_directory}')
        self.project_results()

if __name__ == '__main__':
    filter = Filter()
    filter.main()
