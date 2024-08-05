# c. 2024 solutionst.com llc
# This code is licensed under the MIT license (See LICENSE for details).
import os
import sys
import pandas as pd
import csv
import datetime
import constants

class ProcessEvents(object):
    
    DATETIME_FMT = '%m/%d/%Y %H:%M'
    DATE_FMT = '%m/%d/%Y'
    DATE_FMT_PD = '%Y-%m-%d'
    SCRIPT_DIRECTORY = os.path.dirname(os.path.abspath(sys.argv[0]))

    def __init__(self):
        print ('Event.__init__')
 
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
 
    def calculate_age(self, dob, some_date):
        result = some_date.year - dob.year - ((some_date.month, some_date.day) < (dob.month, dob.day))
        return result
    
    def sort_detail_and_save(self):
        df = pd.read_csv(self.temp_detail_name)
        df = df.sort_values(['mrn', 'collection_date'], ascending=[True, True])
        df.to_csv(self.out_file_name, index=False)
        return df

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
                    work_date = datetime.datetime.strptime(dob_str, ProcessEvents.DATETIME_FMT)
                    dob = work_date.date()
                    deceased_date = datetime.date(1900, 1, 1)
                    dead_str = row[7]
                    if dead_str not in ('NULL', '00:00.0'):
                        try:
                            work_date = datetime.datetime.strptime(dead_str, evt.DATE_FMT_PD)
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
      
    def load_data_to_df(self, in_file_name):
        df_raw = pd.read_csv(in_file_name)
        print(df_raw.shape)
        df_raw = df_raw.drop(columns = ['PatientID', 'EncounterID', 'ACCESSION_NUMBER', 'ORDER_DATE', 'ORDER_NAME', 'OBSERVATION_DATE', 'ActivityDate', 'RESULT_NAME', 'RANGE', 'CLARITY_ORDER_CODE'], errors='ignore')
        df_raw = df_raw.drop(df_raw[df_raw.ORDER_CODE == 'SPECIMEN'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'CLINF'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'ADEQ'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'GROSS'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'COMMENT'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'COMMENTS'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'TGYN_URL'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'HPVD_URL'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == '88142'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'SPF'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'TDGYN'].index)
        df_raw = df_raw.drop(df_raw[df_raw.ORDER_CODE == 'GYN'].index)
        df_raw = df_raw.drop(df_raw[df_raw.ORDER_CODE == 'GADD'].index)
        df_raw = df_raw.drop(df_raw[df_raw.ORDER_CODE == 'FNAS'].index)
        df_raw = df_raw.drop(df_raw[df_raw.VALUE == ' DNR'].index)
        df_raw = df_raw[~df_raw['VALUE'].str.startswith(' http://')]
        df_raw = df_raw[~df_raw['VALUE'].str.startswith(' https://')]
        df_raw['COLLECTION_DATE'] = pd.to_datetime(df_raw['COLLECTION_DATE'], format=ProcessEvents.DATETIME_FMT).dt.date
        df_raw = df_raw.sort_values(['mrn', 'COLLECTION_DATE'], ascending=[True, True])
        return df_raw

    def print_summary(self, df):
        print('Summary:')
        print(df.shape)
        print(df.head())

    def save_temp_to_csv(self, df, file_name):
        df.to_csv(file_name, index=False)

    def choose_hpv_value(self, old_val, new_val):
        # print(f'choose_hpv_value called with old {old_val} and new {new_val}')
        if new_val.strip() == 'Detected':
            return new_val
        if old_val.strip().startswith('Invalid'):
            return new_val
        return old_val

    def code_hpv_value(self, key, hpv_value_dict):
        if key in hpv_value_dict:
            work = hpv_value_dict[key]
            if work == 'Detected':
                return 'Pos'
            elif work == 'Not detected':
                return 'Neg'
            else:
                return ''
   
    def make_hpv_comment(self, hpv_value_dict):
        work = ''
        for key in hpv_value_dict:
            val = hpv_value_dict[key]
            work = work + '#' + key +': ' + val
        return work
    
    def determine_results(age, cyto_value_list, hpv_value_dict):
        return []
    
    def output_row(self, mrn_for_row, last_row, csv_writer, cyto_value_list, hpv_value_dict):
        # capture the mrn for control of the colpo and leep files
        if mrn_for_row not in self.screening_mrn:
            self.screening_mrn.add(mrn_for_row)
        last_date_str = last_row[1]
        last_collection_date = datetime.datetime.strptime(last_date_str, ProcessEvents.DATE_FMT_PD)
        facts = self.mrn_facts[mrn_for_row]
        dob = facts[0]
        age = self.calculate_age(dob, last_collection_date)
        result_code = last_row[3]
        # ('mrn', 'collection_date', 'cyto_result', 'hpvdna_result', 'hpv_other', 'hpv16', 'hpv18', 'followup', 'dob', 'age', 'comment')
        result_tuple = self.determine_results(age, cyto_value_list, hpv_value_dict)
        coll_date_str = datetime.datetime.strftime(last_collection_date, ProcessEvents.DATE_FMT_PD)
        dob_str = datetime.datetime.strftime(dob, ProcessEvents.DATE_FMT_PD)
        result_row = (mrn_for_row, coll_date_str, result_tuple[0], result_tuple[1], result_tuple[2], result_tuple[3], \
                        result_tuple[4], result_tuple[5], dob_str, age, result_tuple[6])
        csv_writer.writerow(result_row)

    def summarize_results(self):
        print("summarize_results")
        line_count = 0
        cyto_value_list = list()
        hpv_value_dict = dict()
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
                        collection_date = datetime.datetime.strptime(date_str, ProcessEvents.DATE_FMT_PD)
                        order_code = row[2]
                        result_code = row[3]
                        value = row[4]
                        if mrn != last_mrn or collection_date != last_date:
                            if last_mrn != '':
                                # new patient or date - output values
                                self.output_row(last_mrn, last_row, out_writer, cyto_value_list, hpv_value_dict)
                                cyto_value_list.clear()
                                hpv_value_dict.clear()
                            last_mrn = mrn
                            last_date = collection_date
                        if order_code in ['TGYNS', 'TDGYNS', 'CYTONG']:
                            cyto_value_list.append(value.strip())
                        elif order_code == 'HPVDNA':
                            if result_code not in hpv_value_dict:
                                hpv_value_dict[result_code] = value.strip()
                            else:
                                old_val = hpv_value_dict[result_code]
                                if old_val != value:
                                    hpv_value_dict[result_code] = self.choose_hpv_value(old_val, value)
                                    # print(f'resolved result code value at line {line_count} to {hpv_value_dict[result_code]}')
                        else:
                            print(f'unexpected order_code {order_code} for {mrn}')
                        last_row = row
                self.output_row(last_mrn, last_row, out_writer, cyto_value_list, hpv_value_dict)
            print(f'Processed {line_count} screened lines.')
    
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
                            the_date = datetime.datetime.strptime(date_str, ProcessEvents.DATETIME_FMT).date()
                            result_date = datetime.datetime.strftime(the_date, ProcessEvents.DATE_FMT_PD)
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
        for i in range(self.mrn_max_count):
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

