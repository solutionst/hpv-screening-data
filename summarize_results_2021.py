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
        self.cyto_values = list()
        self.hpv_values = dict()
        self.dob_dict = dict()

    def make_dob_dict(self):
        line_count = 0
        with open(os.path.join(self.csv_directory, 'hpi10029_patient_demographics_2021_to_2023.csv'), 'r') as f:
            line_count = 0
            reader = csv.reader(f)
            for row in reader:
                if line_count == 0:
                    # skip header line
                    line_count += 1
                else:
                    line_count += 1
                    mrn = row[2]
                    dob_str = row[6]
                    dob = datetime.datetime.strptime(dob_str, self.datetime_fmt)
                    self.dob_dict[mrn] = dob.date()
        print(f'Processed make_dob_dict {line_count} lines.')

    def calculate_age(self, dob, some_date):
        result = some_date.year - dob.year - ((some_date.month, some_date.day) < (dob.month, dob.day))
        return result
    
    def code_hpv_value(self, key):
        if key in self.hpv_values:
            work = self.hpv_values[key]
            if work == 'Detected':
                return 'Y'
            elif work == 'Not detected':
                return 'N'
            else:
                return ''
    
    def make_hpv_comment(self):
        work = ''
        for key in self.hpv_values:
            val = self.hpv_values[key]
            work = work + '#' + key +': ' + val
        return work
    
    def determine_results(self, age):
        cyto = 'Unknown'
        hpv = ''
        hpv_other = ''
        hpv16 = ''
        hpv18 = ''
        screen = 'Screen_Unknown'
        comment = ''
        # cytology encoding
        for val in self.cyto_values:
            if 'ASC-H' in val:
                cyto = 'ASCH'
                break
            elif 'ASC-US' in val:
                cyto = 'ASCUS'
                break
            elif 'Low grade squamous intraepithelial lesion' in val:
                cyto = 'LSIL'
                break
            elif 'Negative for intraepithelial lesion or malignancy' in val:
                cyto = 'NILM'
                break
        if cyto == 'Unknown':
            comment = "#".join(self.cyto_values)
        # hpv encoding
        if len(self.hpv_values) > 0:
            hpv_other = self.code_hpv_value('HPVOHR')
            hpv16 = self.code_hpv_value('HPV16')
            hpv18 = self.code_hpv_value('HPV18')
            if 'HPVR' in self.hpv_values:
                work = self.hpv_values['HPVR']
                if work is None:
                    hpv = ''
                elif 'High Risk Human Papillomavirus detected' in work:
                    hpv = 'Y'
                elif 'High Risk Human Papillomavirus not detected' in work:
                    hpv = 'N'
                else:
                    hpv = ''
            else:
                hpv = ''
            if hpv_other == '' or hpv16 == '' or hpv18 == '' or hpv == '':
                hpv_comment = self.make_hpv_comment()
                comment = comment + hpv_comment
        else:
            comment = comment + '#No HPV results present'

        # screening result
        # age 30 and over
        if age >= 30:
            if (hpv16 == 'Y' or hpv18 == 'Y'):
                screen = 'High'
            elif cyto == 'ASCH':
                screen = 'High'
            elif hpv_other == 'Y' and 'ASC' in cyto:
                screen = 'High'
            elif hpv_other == 'Y' and cyto != 'NILM':
                screen = 'High'
            elif hpv_other == 'Y' and cyto == "NILM":
                screen = 'Low-1'
            elif cyto == 'ASCUS' and hpv == '':
                screen = 'Low-1'
            elif hpv == 'N' and cyto == 'LSIL':
                screen = 'Low-3'
            elif hpv == 'N' and cyto == 'ASCUS':
                screen = 'Normal-5'
            elif hpv == 'N' and cyto == 'NILM':
                screen = 'Normal-5'
            elif cyto == 'Unknown' and hpv == 'N':
                screen = 'Unsat-HPV-neg'
            elif cyto == 'NILM' and hpv == '':
                screen = 'Normal-3'
            elif cyto == 'LSIL' and hpv == '':
                screen = 'Low-1'
            elif cyto == 'Unknown' and hpv == '':
                screen = 'Unsat'
        else:
            if (hpv16 == 'Y' or hpv18 == 'Y'):
                screen = 'High'
            elif cyto == 'ASCH':
                screen = 'High'
            elif cyto == 'NILM' and hpv_other == 'Y':
                screen = 'Low-1'
            elif cyto == 'NILM' and hpv == '':
                screen = 'Normal-3'
            elif cyto == 'NILM' and hpv == 'N':
                screen = 'Normal-3'
            elif cyto == 'Unknown' and hpv == '':
                screen = 'Unsat'
            elif cyto == 'ASCUS' and hpv == 'N':
                screen = 'Normal-3'
            elif cyto == 'ASCUS' and hpv == 'Y':
                screen = 'High'
            elif cyto == 'Unknown' and hpv == 'N':
                screen = 'Unsat-HPV-neg'
            elif cyto == 'LSIL':
                screen = 'Low-1'
            elif cyto == 'ASCUS' and hpv == '':
                screen = 'Low-1'
            elif cyto == 'Unknown' and hpv == 'Y':
                screen = 'Low-1'
                
        return cyto, hpv, hpv_other, hpv16, hpv18, screen, comment

    def choose_hpv_value(self, old_val, new_val):
        # print(f'choose_hpv_value called with old {old_val} and new {new_val}')
        if new_val.strip() == 'Detected':
            return new_val
        if old_val.strip().startswith('Invalid'):
            return new_val
        return old_val
    
    def output_row(self, mrn_for_row, last_row, csv_writer):
        last_date_str = last_row[2]
        last_collection_date = datetime.datetime.strptime(last_date_str, self.date_fmt)
        encounter_id = last_row[1]
        dob = self.dob_dict[mrn_for_row]
        age = self.calculate_age(dob, last_collection_date)
        result_code = last_row[5]
        # 'mrn', 'encounter_id', 'cyto_result', 'hpvdna_result', 'hpv_other', 'hpv16', 'hpv18', 'screening_result', 'collection_date', 'comment'
        result_tuple = self.determine_results(age)
        coll_date_str = datetime.datetime.strftime(last_collection_date, self.date_fmt)
        dob_str = datetime.datetime.strftime(dob, self.date_fmt)
        result_row = (mrn_for_row, encounter_id, result_tuple[0], result_tuple[1], result_tuple[2], result_tuple[3], \
                        result_tuple[4], result_tuple[5], coll_date_str, dob_str, age, result_tuple[6])
        csv_writer.writerow(result_row)
        # clear collection buckets
        self.cyto_values.clear()
        self.hpv_values.clear()

    def summarize_results(self):
        print("summarize_results")
        line_count = 0
        with open(os.path.join(self.csv_directory, 'results_2021_sorted.csv'), 'r', encoding='utf-8') as f:
            with open (os.path.join(self.csv_directory, 'results_2021_collapsed.csv'), 'w', newline='', encoding='utf-8') as out:
                out_writer = csv.writer(out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                # mrn,PatientID,EncounterID,ACCESSION_NUMBER,ORDER_DATE,COLLECTION_DATE,OBSERVATION_DATE,ActivityDate,ORDER_CODE,ORDER_NAME,RESULT_CODE,RESULT_NAME,VALUE,RANGE,RESULT_COMMENT
                header = ('mrn', 'encounter_id', 'cyto_result', 'hpvdna_result', 'hpv_other', 'hpv16', 'hpv18', 'screening_result', 'collection_date', 'dob', 'age', 'comment')
                out_writer.writerow(header)
                line_count = 0
                reader = csv.reader(f)
                last_mrn = ''
                last_encounter = ''
                last_date = datetime.date(1900, 1, 1)
                for row in reader:
                    if line_count == 0:
                        # skip header line
                        line_count += 1
                    else:
                        line_count += 1
                        mrn = row[0]
                        date_str = row[2]
                        collection_date = datetime.datetime.strptime(date_str, self.date_fmt)
                        order_code = row[3]
                        value = row[7]
                        result_code = row[5]
                        if mrn != last_mrn or collection_date != last_date:
                            if last_mrn != '':
                                # new patient or date - output values
                                self.output_row(last_mrn, last_row, out_writer)
                            last_mrn = mrn
                            last_date = collection_date
                        if order_code in ['TGYNS', 'TDGYNS', 'CYTONG']:
                            self.cyto_values.append(value.strip())
                        elif order_code == 'HPVDNA':
                            if result_code not in self.hpv_values:
                                self.hpv_values[result_code] = value.strip()
                            else:
                                old_val = self.hpv_values[result_code]
                                if old_val != value:
                                    self.hpv_values[result_code] = self.choose_hpv_value(old_val, value)
                                    # print(f'resolved result code value at line {line_count} to {self.hpv_values[result_code]}')
                        else:
                            print(f'unexpected order_code {order_code} for {mrn}')
                        last_row = row
                self.output_row(last_mrn, last_row, out_writer)
            print(f'Processed {line_count} screened lines.')    

    def main(self):
        print("main")
        print(f'csv_directory: {self.csv_directory}')
        self.make_dob_dict()
        self.summarize_results()

if __name__ == '__main__':
    filter = Filter()
    filter.main()
