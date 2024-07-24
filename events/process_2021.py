# c. 2024 solutionst.com llc
# This code is licensed under the MIT license (See LICENSE for details).
import os
import sys
import pandas as pd
import csv
import datetime
import constants

class Lab2021:
    def __init__(self):
        print("__init__")
        self.script_directory = os.path.dirname(os.path.abspath(sys.argv[0]))
        # mac
        self.in_directory = os.path.join('/Volumes', 'KINGSTON', 'original','2021-2023')
        self.out_directory = os.path.join('/Volumes', 'KINGSTON', 'out', '2021')
        # windows
        # self.in_directory = os.path.join('d:','original','2021-2023')
        # self.out_directory = os.path.join('d:', 'out', '2021')

        self.in_file_name = os.path.join(self.in_directory, 'hpi10029_cervical_and_Cyto_labresults_2021_to_2023.csv')
        self.out_file_name = os.path.join(self.out_directory, 'screen_details.csv')
        self.demographic_file_name = os.path.join(self.in_directory, 'hpi10029_patient_demographics_2021_to_2023.csv')
        self.cyto_file_name = os.path.join(self.out_directory, constants.EventConstants.CYTO_FILE_NAME)
        self.hpvdna_file_name = os.path.join(self.out_directory, constants.EventConstants.HPVDNA_FILE_NAME)
        self.followup_file_name = os.path.join(self.out_directory, constants.EventConstants.FOLLOWUP_FILE_NAME)
        self.colpo_file_name = os.path.join(self.out_directory, constants.EventConstants.COLPO_FILE_NAME)
        self.leep_file_name = os.path.join(self.out_directory, constants.EventConstants.LEEP_FILE_NAME)
        self.dob_file_name  = os.path.join(self.out_directory, constants.EventConstants.DOB_FILE_NAME)
        self.merged_events_name = os.path.join(self.out_directory, constants.EventConstants.MERGED_FILE_NAME)
        self.wide_file_name = os.path.join(self.out_directory, constants.EventConstants.WIDE_FILE_NAME)

        self.leep_in_file_name = os.path.join(self.in_directory, 'hpi10029_LEEP_Labresults_2021_to_2023.csv')
        self.colpo_in_file_name = os.path.join(self.in_directory, 'hpi10029_colpo_labresults_2021_to_2023.csv')

        self.datetime_fmt = '%m/%d/%Y %H:%M'
        self.date_fmt = '%m/%d/%Y'
        
        self.cyto_values = list()
        self.hpv_values = dict()
        self.mrn_facts = dict()
        self.screening_mrn = set()

        self.temp_file_name = os.path.join(self.out_directory, 'temp_screen.csv')
        self.temp_detail_name = os.path.join(self.out_directory, 'temp_screen_details.csv')

    def load_data(self):
        df_raw = pd.read_csv(self.in_file_name)
        print(df_raw.shape)
        self.df = df_raw.drop(columns = ['PatientID', 'EncounterID', 'ACCESSION_NUMBER', 'ORDER_DATE', 'ORDER_NAME', 'OBSERVATION_DATE', 'ActivityDate', 'RESULT_NAME', 'RESULT_COMMENT', 'RANGE'])
        # if order_code != 'SPECIMEN' and result_code not in ['CLINF', 'ADEQ', 'GROSS', 'COMMENT', 'COMMENTS']:
        self.df = self.df.drop(self.df[self.df.ORDER_CODE == 'SPECIMEN'].index)
        self.df = self.df.drop(self.df[self.df.RESULT_CODE == 'CLINF'].index)
        self.df = self.df.drop(self.df[self.df.RESULT_CODE == 'ADEQ'].index)
        self.df = self.df.drop(self.df[self.df.RESULT_CODE == 'GROSS'].index)
        self.df = self.df.drop(self.df[self.df.RESULT_CODE == 'COMMENT'].index)
        self.df = self.df.drop(self.df[self.df.RESULT_CODE == 'COMMENTS'].index)
        self.df = self.df.drop(self.df[self.df.RESULT_CODE == 'TGYN_URL'].index)
        self.df = self.df.drop(self.df[self.df.RESULT_CODE == 'HPVD_URL'].index)
        self.df = self.df.drop(self.df[self.df.RESULT_CODE == '88142'].index)
        self.df = self.df.drop(self.df[self.df.VALUE == ' DNR'].index)
        self.df = self.df[~self.df['VALUE'].str.startswith(' http://')]
        self.df = self.df[~self.df['VALUE'].str.startswith(' https://')]
        self.df = self.df.sort_values(['mrn', 'COLLECTION_DATE'], ascending=[True, True])

    def sort_detail_and_save(self):
        df = pd.read_csv(self.temp_detail_name)
        df = df.sort_values(['mrn', 'collection_date'], ascending=[True, True])
        df.to_csv(self.out_file_name, index=False)
        return df

    def print_summary(self):
        print('Summary:')
        print(self.df.shape)
        print(self.df.head())

    def save_temp_to_csv(self):
        self.df.to_csv(self.temp_file_name, index=False)

    def determine_study_race(self, source_race):
        result = 'unknown_race'
        if source_race == 'American Indian and Alaska Native':
            result =  'AIAN'
        elif source_race in ('Asian', 'Chinese', 'Japanese', 'Korean', 'Vietnamese', 'Asian, Chinese', 'Asian, Japanese', 'Asian, Korean', 'Asian, Vietnamese', 'Chinese, Vietnamese', 'Asian, Other Asian', 'Chinese, Other Asian', 'Japanese, Korean', 'Other Asian, Chinese', 'Other Asian', 'Japanese, Other Asian', 'Chinese, Other Asian, Vietnamese'):
            result =  'Asian'
        elif source_race == 'Asian Indian':
            result =  'Asian Indian'
        elif source_race == 'Black or African American':
            result = 'Black'
        elif 'Black or African American' in source_race:
            result = 'Black-Mixed'
        elif source_race == 'White or Caucasian':
            result = 'White'
        elif source_race == 'Choose not to disclose':
            result = 'Not-Disclosed'
        elif source_race in ('Native Hawaiian', 'Filipino', 'Native Hawaiian, Other Pacific Islander', 'Other Pacific Islander', 'Native Hawaiian and Other Pacific Islander', 'Guamanian or Chamorro', 'Native Hawaiian and Other Pacific Islander, Filipino', 'Native Hawaiian and Other Pacific Islander, Native Hawaiian'):
            result = 'NHPI'
        elif source_race in ('Middle Eastern/North African'): 
            result = 'MENA'
        elif source_race in ('Asian, White or Caucasian', 'Other, White or Caucasian', 'American Indian and Alaska Native, Asian', 'American Indian and Alaska Native, Chinese, White or Caucasian', 'American Indian and Alaska Native, Choose not to disclose', 'American Indian and Alaska Native, Other', 'American Indian and Alaska Native, White or Caucasian', 'American Indian and Alaska Native, Guamanian or Chamorro', 'American Indian and Alaska Native, Korean, White or Caucasian', 'American Indian and Alaska Native, Other, White or Caucasian', 'Asian Indian, Chinese', 'Asian Indian, Other Asian', 'Asian Indian, Unknown', 'Asian Indian, White or Caucasian', 'Asian, American Indian and Alaska Native', 'Asian, Asian Indian', 'Asian, Choose not to disclose', 'Asian, Filipino', 'Asian, Middle Eastern/North African', 'Asian, Native Hawaiian and Other Pacific Islander', 'Asian, Other', 'Asian, Other Pacific Islander', 'Asian, Other, Unknown', 'Asian, Other, White or Caucasian', 'Asian, Unknown', 'Asian, Native Hawaiian and Other Pacific Islander, Japanese', 'Chinese, Filipino', 'Chinese, Filipino, Other', 'Chinese, Filipino, White or Caucasian', 'Chinese, Japanese, White or Caucasian', 'Chinese, Other', 'Chinese, White or Caucasian', 'Choose not to disclose, White or Caucasian', 'Filipino, White or Caucasian', 'Japanese, White or Caucasian', 'Korean, White or Caucasian', 'Middle Eastern/North African, White or Caucasian', 'White or Caucasian, Unknown', 'White or Caucasian, Other', 'White or Caucasian, Asian', 'White or Caucasian, American Indian and Alaska Native', 'Unknown, White or Caucasian', 'Other Asian, White or Caucasian'):
            result = 'Mixed'
        elif source_race == 'Other':
            result = 'Other'
        elif source_race in ('Unknown', 'NULL'):
            result = 'Unknown'
        else:
            result = 'Mixed'
        return result

    def make_mrn_facts(self):
        line_count = 0
        with open(self.demographic_file_name, 'r') as f:
            line_count = 0
            reader = csv.reader(f)
            for row in reader:
                if line_count == 0:
                    # skip header line
                    line_count += 1
                else:
                    mrn = row[2]
                    s = row[3]
                    lastname = s if s != 'NULL' else ''
                    s = row[4]
                    firstname = s if s != 'NULL' else ''
                    s = row[5]
                    middlename = s if s != 'NULL' else ''
                    dob_str = row[6]
                    work_date = datetime.datetime.strptime(dob_str, self.datetime_fmt)
                    dob = work_date.date()
                    deceased_date = datetime.date(1900, 1, 1)
                    dead_str = row[7]
                    if dead_str not in ('NULL', '00:00.0'):
                        try:
                            work_date = datetime.datetime.strptime(dead_str, self.datetime_fmt)
                            deceased_date = work_date.date()
                        except:
                            print('dead_str exception: ' + dead_str)
                    source_race = row[11]
                    study_race = self.determine_study_race(source_race)
                    ethnicity = row[12]
                    postalcode = row[19][:5]
                    homephone = row[20]
                    mobilephone = row[21]
                    email = row[22]

                    ## 'dob', 'study_race', 'source_race', 'ethnicity', 'lastname', 'firstname', 'middlename', 'postalcode', 'homephone', 'mobilephone', 'email'
                    facts = [dob, study_race, source_race, ethnicity, lastname, firstname, middlename, postalcode, homephone, mobilephone, email]
                    self.mrn_facts[mrn] = facts
        print(f'Processed make_mrn_facts {line_count} lines.')

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
        # capture the mrn for control of the colpo and leep files
        if mrn_for_row not in self.screening_mrn:
            self.screening_mrn.add(mrn_for_row)
        last_date_str = last_row[1]
        last_collection_date = datetime.datetime.strptime(last_date_str, self.datetime_fmt)
        facts = self.mrn_facts[mrn_for_row]
        dob = facts[0]
        age = self.calculate_age(dob, last_collection_date)
        result_code = last_row[3]
        # ('mrn', 'collection_date', 'cyto_result', 'hpvdna_result', 'hpv_other', 'hpv16', 'hpv18', 'followup', 'dob', 'age', 'comment')
        result_tuple = self.determine_results(age)
        coll_date_str = datetime.datetime.strftime(last_collection_date, self.date_fmt)
        dob_str = datetime.datetime.strftime(dob, self.date_fmt)
        result_row = (mrn_for_row, coll_date_str, result_tuple[0], result_tuple[1], result_tuple[2], result_tuple[3], \
                        result_tuple[4], result_tuple[5], dob_str, age, result_tuple[6])
        csv_writer.writerow(result_row)
        # clear collection buckets
        self.cyto_values.clear()
        self.hpv_values.clear()

    def summarize_results(self):
        print("summarize_results")
        line_count = 0
        with open(self.temp_file_name, 'r', encoding='utf-8') as f:
            with open (self.temp_detail_name, 'w', newline='', encoding='utf-8') as out:
                out_writer = csv.writer(out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                header = ('mrn', 'collection_date', 'cyto_result', 'hpvdna_result', 'hpv_other', 'hpv16', 'hpv18', 'followup', 'dob', 'age', 'comment')
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
                        date_str = row[1]
                        collection_date = datetime.datetime.strptime(date_str, self.datetime_fmt)
                        order_code = row[2]
                        result_code = row[3]
                        value = row[4]
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

    def create_one_event_file(self, df, drop_list, result_field, event_index, event_name, save_file_name):
        work = df.drop(columns = drop_list)
        work.rename(columns = {'collection_date':'date', result_field:'result'}, inplace = True)
        work['event_idx'] = event_index
        work['event_name'] = event_name
        work2 = work[constants.EventConstants.EVENT_HEADER_LIST]
        print(work2.head())
        work2.to_csv(save_file_name, index = False)

    def create_lab_event_files(self, df):
        self.create_one_event_file(df, \
                                   ['hpvdna_result', 'hpv_other', 'hpv16', 'hpv18', 'followup', 'dob', 'age', 'comment'], \
                                    constants.EventConstants.CYTO_NAME, constants.EventConstants.CYTO_IDX, \
                                    constants.EventConstants.CYTO_NAME, self.cyto_file_name)
        self.create_one_event_file(df, \
                                   ['cyto_result', 'hpv_other', 'hpv16', 'hpv18', 'followup', 'dob', 'age', 'comment'], \
                                    constants.EventConstants.HPVDNA_NAME, constants.EventConstants.HPVDNA_IDX, \
                                    constants.EventConstants.HPVDNA_NAME, self.hpvdna_file_name)

        self.create_one_event_file(df, \
                                   ['hpvdna_result', 'hpv_other', 'hpv16', 'hpv18', 'cyto_result', 'dob', 'age', 'comment'], \
                                    constants.EventConstants.FOLLOWUP_NAME, constants.EventConstants.FOLLOWUP_IDX, \
                                    constants.EventConstants.FOLLOWUP_NAME, self.followup_file_name)

    def process_followup_events(self, in_file_name, out_file_name, event_idx, event_name):
        line_count = 0
        with open(in_file_name, 'r') as f:
            with open (out_file_name, 'w', newline='', encoding='utf-8') as out:
                out_writer = csv.writer(out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                out_writer.writerow(constants.EventConstants.EVENT_HEADER_LIST)
                reader = csv.reader(f)
                for row in reader:
                    if line_count == 0:
                        # skip header line
                        line_count += 1
                    else:
                        line_count += 1
                        mrn = row[0]
                        if mrn in self.screening_mrn:
                            date_str = row[5]
                            the_date = datetime.datetime.strptime(date_str, self.datetime_fmt).date()
                            result_date = datetime.datetime.strftime(the_date, self.date_fmt)
                            tuple = (mrn, result_date, event_idx, event_name, 'performed')
                            out_writer.writerow(tuple)
        print(f'Processed {event_name} for {line_count} lines.')
        # sort the results
        df = pd.read_csv(out_file_name)
        df = df.sort_values(['mrn', 'date', 'event_idx'], ascending=[True, True, True]).drop_duplicates(subset=['mrn', 'date'])
        df.to_csv(out_file_name, index=False)
    
    def consolidate_and_sort(self):
        cyto = pd.read_csv(self.cyto_file_name)
        hpvdna = pd.read_csv(self.hpvdna_file_name)
        followup = pd.read_csv(self.followup_file_name)
        colpo = pd.read_csv(self.colpo_file_name)
        leep = pd.read_csv(self.leep_file_name)
        df = pd.concat([cyto, hpvdna, followup, colpo, leep])
        df = df.sort_values(['mrn', 'date', 'event_idx'], ascending=[True, True, True])
        print(df.head())
        df.to_csv(self.merged_events_name, index = False)
        work = df.groupby(['mrn']).size().reset_index(name='counts')
        print(work[['counts']].describe())

    def make_wide_header(self):
        r = ['mrn', 'dob', 'study_race', 'source_race', 'ethnicity', 'lastname', 'firstname', 'middlename', 'postalcode', 'homephone', 'mobilephone', 'email']
        for i in range(constants.EventConstants.MRN_MAX_COUNT):
            t = 'date_' + str(i + 1).zfill(2)
            r.append(t)
            t = 'event_' + str(i + 1).zfill(2)
            r.append(t)
            t = 'result_' + str(i + 1).zfill(2)
            r.append(t)
        return r

    def output_wide_row(self, writer, mrn, data_list):
        row = [mrn]
        demo_data = self.mrn_facts[mrn]
        row = row + demo_data
        length = len(data_list)
        for i in range(0, length):
            row = row + data_list[i]
        writer.writerow(row)

    def create_wide_file(self):
        header = self.make_wide_header()
        with open(self.merged_events_name, 'r', encoding='utf-8') as f:
            with open (self.wide_file_name, 'w', newline='', encoding='utf-8') as out:
                reader = csv.reader(f)
                writer = csv.writer(out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(header)
                last_mrn = ''
                line_count = 0
                data_list = []
                for row in reader:
                    if line_count == 0:
                        # skip header line
                        line_count += 1
                    else:
                        mrn = row[0]
                        if mrn != last_mrn:
                            if last_mrn != '':
                                self.output_wide_row(writer, last_mrn, data_list)
                                data_list.clear()
                                data_list.append([row[1], row[3], row[4]])
                            else:
                                data_list.append([row[1], row[3], row[4]])
                            last_mrn = mrn
                        else:
                            data_list.append([row[1], row[3], row[4]])
                if len(data_list) > 0:
                    self.output_wide_row(writer, last_mrn, data_list)
                print(f'create_wide_file processed {line_count} screened lines.') 

    def main(self):
        print("main")
        self.make_mrn_facts()

        self.load_data()
        self.print_summary()
        self.save_temp_to_csv()
        self.summarize_results()
        detail_df = self.sort_detail_and_save()
        self.create_lab_event_files(detail_df)
        self.process_followup_events(self.leep_in_file_name, self.leep_file_name, constants.EventConstants.LEEP_IDX, constants.EventConstants.LEEP_NAME)
        self.process_followup_events(self.colpo_in_file_name, self.colpo_file_name, constants.EventConstants.COLPO_IDX, constants.EventConstants.COLPO_NAME)
        self.consolidate_and_sort()
        self.create_wide_file()
        exit()

if __name__ == '__main__':
    worker = Lab2021()
    worker.main()
