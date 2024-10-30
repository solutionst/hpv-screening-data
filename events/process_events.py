# c. 2024 solutionst.com llc
# This code is licensed under the MIT license (See LICENSE for details).
import os
import sys
import pandas as pd
import csv
import datetime
import constants
import logging
import json

class ProcessEvents(object):
    
    DATETIME_FMT = '%m/%d/%Y %H:%M'
    DATE_FMT = '%m/%d/%Y'
    DATE_FMT_PD = '%Y-%m-%d'
    SCRIPT_DIRECTORY = os.path.dirname(os.path.abspath(sys.argv[0]))

    def __init__(self):
        print ('Event.__init__')
    
    def log_create(self):
        # inspired by https://stackoverflow.com/questions/19765139/what-is-the-proper-way-to-do-logging-to-a-csv-file
        try:
            os.remove(self.log_file_name)
        except OSError:
            pass
        # create logger
        self.lgr = logging.getLogger('mrn')
        self.lgr.setLevel(logging.DEBUG) # log all escalated at and above DEBUG
        # add a file handler
        fh = logging.FileHandler(self.log_file_name)
        fh.setLevel(logging.DEBUG) # ensure all messages are logged to file

        # create a formatter and set the formatter for the handler.
        # frmt = logging.Formatter('%(asctime)s,%(name)s,%(levelname)s,%(message)s')
        frmt = logging.Formatter('%(asctime)s,%(name)s,%(levelname)s,%(message)s')
        fh.setFormatter(frmt)

        # add the Handler to the logger
        self.lgr.addHandler(fh)

        # You can now start issuing logging statements in your code
        self.lgr.debug('a debug message 2')
 
    def log_mrn_info(self, mrn, message):
        log_msg = "{},{}".format(mrn, message)
        self.lgr.info(log_msg)

    def log_mrn_warn(self, mrn, message):
        log_msg = "{},{}".format(mrn, message)
        self.lgr.warn(log_msg)

    def log_mrn_error(self, mrn, message):
        log_msg = "{},{}".format(mrn, message)
        self.lgr.error(log_msg)
        
    def log_shutdown(self):
        logging.shutdown()

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
        df = pd.read_csv(self.screen_file_20)
        df = df.sort_values(['mrn', 'collection_date'], ascending=[True, True])
        df.to_csv(self.screen_file_20, index=False)
        return df

    def create_one_event_file(self, df, drop_list, result_field, event_index, event_name, save_file_name):
        work = df.drop(columns = drop_list)
        work.rename(columns = {'COLLECTION_DATE':'date', result_field:'result'}, inplace = True)
        work['event_idx'] = event_index
        work['event_name'] = event_name
        work2 = work[constants.EventConstants.EVENT_HEADER_LIST]
        print(work2.head())
        work2.to_csv(save_file_name, index = False)

    def create_lab_event_files(self, df):
        self.create_one_event_file(df, \
                                   [constants.EventConstants.HPVDNA_NAME, constants.EventConstants.HPVOTHR_NAME, constants.EventConstants.HPV16_NAME, constants.EventConstants.HPV18_NAME, 'followup', 'dob', 'age', 'comment'], constants.EventConstants.CYTO_NAME, constants.EventConstants.CYTO_IDX, constants.EventConstants.CYTO_NAME, self.cyto_file_name)
        self.create_one_event_file(df, \
                                   [constants.EventConstants.CYTO_NAME, constants.EventConstants.HPVOTHR_NAME, constants.EventConstants.HPV16_NAME, constants.EventConstants.HPV18_NAME, 'followup', 'dob', 'age', 'comment'], constants.EventConstants.HPVDNA_NAME, constants.EventConstants.HPVDNA_IDX, constants.EventConstants.HPVDNA_NAME, self.hpvdna_file_name)
        self.create_one_event_file(df, \
                                   [constants.EventConstants.CYTO_NAME, constants.EventConstants.HPVDNA_NAME, constants.EventConstants.HPVOTHR_NAME, constants.EventConstants.HPV16_NAME, 'followup', 'dob', 'age', 'comment'], constants.EventConstants.HPV18_NAME, constants.EventConstants.HPV18_IDX, constants.EventConstants.HPV18_NAME, self.hpv18_file_name)
        self.create_one_event_file(df, \
                                   [constants.EventConstants.CYTO_NAME, constants.EventConstants.HPVOTHR_NAME, constants.EventConstants.HPVDNA_NAME, constants.EventConstants.HPV18_NAME, 'followup', 'dob', 'age', 'comment'], constants.EventConstants.HPV16_NAME, constants.EventConstants.HPV16_IDX, constants.EventConstants.HPV16_NAME, self.hpv16_file_name)
        self.create_one_event_file(df, \
                                   [constants.EventConstants.CYTO_NAME, constants.EventConstants.HPVDNA_NAME, constants.EventConstants.HPV16_NAME, constants.EventConstants.HPV18_NAME, 'followup', 'dob', 'age', 'comment'], \
                                    constants.EventConstants.HPVOTHR_NAME, constants.EventConstants.HPVOTHR_IDX, constants.EventConstants.HPVOTHR_NAME, self.hpvothr_file_name)

        self.create_one_event_file(df, \
                                   [constants.EventConstants.HPVDNA_NAME, constants.EventConstants.HPVOTHR_NAME, constants.EventConstants.HPV16_NAME, constants.EventConstants.HPV18_NAME, constants.EventConstants.CYTO_NAME, 'dob', 'age', 'comment'], constants.EventConstants.FOLLOWUP_NAME, constants.EventConstants.FOLLOWUP_IDX, constants.EventConstants.FOLLOWUP_NAME, self.followup_file_name)
 
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
                    mrn = row[self.demo_mrn]
                    s = row[self.demo_lastname]
                    lastname = s if s != 'NULL' else ''
                    s = row[self.demo_firstname]
                    firstname = s if s != 'NULL' else ''
                    s = row[self.demo_middlename]
                    middlename = s if s != 'NULL' else ''
                    dob_str = row[self.demo_dob]
                    work_date = datetime.datetime.strptime(dob_str, ProcessEvents.DATETIME_FMT)
                    dob = work_date.date()
                    deceased_date = datetime.date(1900, 1, 1)
                    dead_str = row[self.demo_deceased_date]
                    if dead_str not in ('NULL', '00:00.0'):
                        try:
                            work_date = datetime.datetime.strptime(dead_str, evt.DATE_FMT_PD)
                            deceased_date = work_date.date()
                        except:
                            # swallow
                            pass
                    source_race = row[self.demo_source_race]
                    study_race = self.determine_study_race(source_race)
                    ethnicity = row[self.demo_ethnicity]
                    postalcode = row[self.demo_postalcode][:5]
                    homephone = row[self.demo_homephone]
                    mobilephone = row[self.demo_mobilephone]
                    email = row[self.demo_email]

                    ## 'dob', 'study_race', 'source_race', 'ethnicity', 'lastname', 'firstname', 'middlename', 'postalcode', 'homephone', 'mobilephone', 'email'
                    facts = [dob, study_race, source_race, ethnicity, lastname, firstname, middlename, postalcode, homephone, mobilephone, email]
                    if mrn not in self.mrn_facts:
                        self.mrn_facts[mrn] = facts
                    else:
                        existing = self.mrn_facts[mrn]
                        existing_string = ' '.join(str(e) for e in existing)
                        facts_string = ' '.join(str(e) for e in facts)
                        if existing_string == facts_string:
                            self.log_mrn_info(mrn, 'minor change in mrn demographic facts')
                        else:
                            self.log_mrn_warn(mrn, "mrn facts for output changed")
                            self.log_mrn_warn(mrn, "existing:  " + existing_string)
                            self.log_mrn_warn(mrn, "new facts: " + facts_string)
        print(f'Processed make_mrn_facts {line_count} lines.')
      
    def load_data_to_df(self, in_file_name):
        df_raw = pd.read_csv(in_file_name)
        print(df_raw.shape)
        df_raw = df_raw[~df_raw['VALUE'].str.startswith(' http://')]
        df_raw = df_raw[~df_raw['VALUE'].str.startswith(' https://')]
        print(df_raw.shape)
        if '2021-2023' in in_file_name:
            df_raw = df_raw.drop(columns = ['PatientID', 'EncounterID', 'ORDER_DATE', 'ORDER_NAME', 'OBSERVATION_DATE', 'ActivityDate', 'RESULT_NAME', 'RANGE'])
        else:
            df_raw = df_raw.drop(columns = ['PatientID', 'EncounterID', 'ORDER_DATE', 'ORDER_NAME', 'OBSERVATION_DATE', 'ActivityDate', 'RESULT_NAME', 'RANGE', 'CLARITY_ORDER_CODE'])
        df_raw['COLLECTION_DATE'] = pd.to_datetime(df_raw['COLLECTION_DATE'], format=ProcessEvents.DATETIME_FMT).dt.date
        df_raw = df_raw[['mrn', 'COLLECTION_DATE', 'ACCESSION_NUMBER', 'ORDER_CODE', 'RESULT_CODE', 'VALUE', 'RESULT_COMMENT']]
        df_raw = df_raw.sort_values(['mrn', 'ACCESSION_NUMBER', 'COLLECTION_DATE', 'ORDER_CODE', 'RESULT_CODE'], ascending=[True, True, True, True, True])
        df_raw = df_raw.drop(df_raw[df_raw.ORDER_CODE == 'SPECIMEN'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'CLINF'].index)
        # df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'ADEQ'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'GROSS'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'COMMENT'].index)
        # df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'COMMENTS'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'TGYN_URL'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'HPVD_URL'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == '88142'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'SPF'].index)
        df_raw = df_raw.drop(df_raw[df_raw.RESULT_CODE == 'TDGYN'].index)
        df_raw = df_raw.drop(df_raw[df_raw.ORDER_CODE == 'GYN'].index)
        df_raw = df_raw.drop(df_raw[df_raw.ORDER_CODE == 'GADD'].index)
        df_raw = df_raw.drop(df_raw[df_raw.ORDER_CODE == 'FNAS'].index)
        df_raw = df_raw.drop(df_raw[df_raw.VALUE == ' DNR'].index)
        print(df_raw.shape)
        return df_raw

    def print_summary(self, df):
        print('Summary:')
        print(df.shape)
        print(df.head())

    def save_temp_to_csv(self, df, file_name):
        df.to_csv(file_name, index=False)

    def make_dict_comment(self, hpv_value_dict):
        work = json.dumps(hpv_value_dict)
        return work
    
    def test_cyto_ok(self, cyto_value_dict):
        if len(cyto_value_dict) <= 1:
            return True
        # try to remove unsat accessions from dictionary and re-test
        keys = cyto_value_dict.keys()
        delete_keys = list()
        for accession_key in keys:
            access_dict = cyto_value_dict[accession_key]
            for key, val in access_dict.items():
                if 'UNSAT' in val.upper():
                    delete_keys.append(accession_key)
                    break
        for del_key in delete_keys:
            del cyto_value_dict[del_key]
        if len(cyto_value_dict) <= 1:
            return True
        return False 
    
    def test_hpv_ok(self, hpv_value_dict):
        if len(hpv_value_dict) <= 1:
            return True
        # look for This specimen source has not been validated in a comment and remove accession
        # log MRN as warnging
        return False
    
    def test_results_ok(self, mrn, cyto_value_dict, hpv_value_dict):
        cyto_ok = self.test_cyto_ok(cyto_value_dict)
        hpv_ok = self.test_hpv_ok(hpv_value_dict)
        if cyto_ok and hpv_ok:
            return True
        else:
            msg = ''
            if not cyto_ok:
                msg += '-cyto values bad'
            if not hpv_ok:
                msg += '-hpv values bad'
            self.log_mrn_error(mrn, msg)
            return False

    def code_hpv_value(self, mrn, key, hpv_value_dict):
        if len(hpv_value_dict) == 0:
            self.log_mrn_info(mrn, 'No HPV data')
            return ''
        elif len(hpv_value_dict) == 1:
            only_key = list(hpv_value_dict.keys())[0]
            single_dict = hpv_value_dict[only_key]
            if key in single_dict:
                work = single_dict[key]
                if work == 'Detected':
                    return 'Pos'
                elif work == 'Not detected':
                    return 'Neg'
                else:
                    return ''
        else:
            self.log_mrn_info(mrn, 'Multiple HPV accessions')
            return ''

    def code_hpv_result(self, mrn, hpv_value_dict):
        if len(hpv_value_dict) == 0:
            self.log_mrn_info(mrn, 'No HPV data')
            return ''
        elif len(hpv_value_dict) == 1:
            only_key = list(hpv_value_dict.keys())[0]
            single_dict = hpv_value_dict[only_key]
            if 'HPVR' in single_dict:
                work = single_dict['HPVR']
                if work is None:
                    return ''
                elif 'High Risk Human Papillomavirus detected' in work:
                    return 'Pos'
                elif 'High Risk Human Papillomavirus not detected' in work:
                    return 'Neg'
                else:
                    return ''
        else:
            self.log_mrn_info(mrn, 'Multiple HPV accessions - code_hpv_result')
            return ''
      
    def output_row(self, mrn_for_row, last_row, csv_writer, cyto_value_dict, hpv_value_dict):
        # capture the mrn for control of the colpo and leep files
        if mrn_for_row not in self.screening_mrn:
            self.screening_mrn.add(mrn_for_row)
        last_date_str = last_row[1]
        last_collection_date = datetime.datetime.strptime(last_date_str, ProcessEvents.DATE_FMT_PD)
        facts = self.mrn_facts[mrn_for_row]
        dob = facts[0]
        age = self.calculate_age(dob, last_collection_date)
        # result_code = last_row[3]
        if self.test_results_ok(mrn_for_row, cyto_value_dict, hpv_value_dict):
            # ('mrn', 'collection_date', constants.EventConstants.CYTO_NAME, constants.EventConstants.HPVDNA_NAME, constants.EventConstants.HPVOTHR_NAME, constants.EventConstants.HPV16_NAME, constants.EventConstants.HPV18_NAME, 'followup', 'dob', 'age', 'comment')
            result_tuple = self.determine_results(mrn_for_row, age, cyto_value_dict, hpv_value_dict)
            coll_date_str = datetime.datetime.strftime(last_collection_date, ProcessEvents.DATE_FMT_PD)
            dob_str = datetime.datetime.strftime(dob, ProcessEvents.DATE_FMT_PD)
            result_row = (mrn_for_row, coll_date_str, result_tuple[0], result_tuple[1], result_tuple[2], result_tuple[3], \
                            result_tuple[4], result_tuple[5], dob_str, age, result_tuple[6])
            csv_writer.writerow(result_row)

    def add_cyto_value(self, accession, result_code, value, cyto_value_dict):
        if 'The specimen has been received and the requested test will be ordered' in value:
            return
        if 'RECEIVED' == value.upper():
            return
        if 'Performing Site:' in value:
            return
        if accession not in cyto_value_dict:
            cyto_value_dict[accession] = dict()
        if result_code not in cyto_value_dict[accession]:
            cyto_value_dict[accession][result_code] = value.strip()

    def add_hpv_value(self, accession, result_code, value, hpv_value_dict):
        if accession not in hpv_value_dict:
            hpv_value_dict[accession] = dict()
        if result_code not in hpv_value_dict[accession]:
            hpv_value_dict[accession][result_code] = value.strip()
        else:
            print('mrn in error')

    def summarize_results(self):
        print("summarize_results")
        line_count = 0
        cyto_value_dict = dict()
        hpv_value_dict = dict()
        with open(self.screen_file_10, 'r', encoding='utf-8') as f:
            with open (self.screen_file_20, 'w', newline='', encoding='utf-8') as out:
                out_writer = csv.writer(out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                header = ('mrn', 'collection_date', constants.EventConstants.CYTO_NAME, constants.EventConstants.HPVDNA_NAME, constants.EventConstants.HPVOTHR_NAME, constants.EventConstants.HPV16_NAME, constants.EventConstants.HPV18_NAME, 'followup', 'dob', 'age', 'comment')
                out_writer.writerow(header)
                line_count = 0
                reader = csv.reader(f)
                last_mrn = ''
                last_date = datetime.date(1900, 1, 1)
                last_accession = ''
                for row in reader:
                    if line_count == 0:
                        # skip header line
                        line_count += 1
                    else:
                        line_count += 1
                        mrn = row[0]
                        date_str = row[1]
                        work_date = datetime.datetime.strptime(date_str, ProcessEvents.DATE_FMT_PD)
                        collection_date = work_date.date()
                        accession = row[2]
                        order_code = row[3]
                        result_code = row[4]
                        value = row[5]
                        if 'SEE TEXT' in value.upper():
                            value = row[6]
                        delta = collection_date - last_date
                        if mrn != last_mrn or delta.days >constants.EventConstants.DELTA_DAYS_CYTO_HPV_SAME:
                            if last_mrn != '':
                                # new patient or date - output values
                                self.output_row(last_mrn, last_row, out_writer, cyto_value_dict, hpv_value_dict)
                                cyto_value_dict.clear()
                                hpv_value_dict.clear()
                            last_mrn = mrn
                            last_date = collection_date
                        if order_code in ['TGYNS', 'TDGYNS', 'CYTONG', 'TGYN']:
                            self.add_cyto_value(accession, result_code, value.strip(), cyto_value_dict)
                        elif order_code == 'HPVDNA':
                            self.add_hpv_value(accession, result_code, value.strip(), hpv_value_dict)
                        else:
                            print(f'unexpected order_code {order_code} for {mrn}')
                        last_row = row
                self.output_row(last_mrn, last_row, out_writer, cyto_value_dict, hpv_value_dict)
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
        df = df.sort_values(['mrn', 'collection_date', 'event_idx'], ascending=[True, True, True]).drop_duplicates(subset=['mrn', 'collection_date'])
        df.to_csv(out_file_name, index=False)
    
    def consolidate_and_sort(self):
        cyto = pd.read_csv(self.cyto_file_name)
        hpvdna = pd.read_csv(self.hpvdna_file_name)

        hpv18 = pd.read_csv(self.hpv18_file_name)
        hpv16 = pd.read_csv(self.hpv16_file_name)
        hpvothr = pd.read_csv(self.hpvothr_file_name)

        followup = pd.read_csv(self.followup_file_name)
        colpo = pd.read_csv(self.colpo_file_name)
        leep = pd.read_csv(self.leep_file_name)
        df = pd.concat([cyto, hpvdna, hpv18, hpv16, hpvothr, followup, colpo, leep])
        df = df.sort_values(['mrn', 'collection_date', 'event_idx'], ascending=[True, True, True])
        print(df.head())
        df.to_csv(self.merged_events_name, index = False)
        work = df.groupby(['mrn']).size().reset_index(name='counts')
        print(work[['counts']].describe(percentiles=[.1, .2, .3, .4, .5, .6, .7, .8, .9, .95]))

    def make_wide_header(self):
        r = ['mrn', 'dob', 'study_race', 'source_race', 'ethnicity', 'lastname', 'firstname', 'middlename', 'postalcode', 'homephone', 'mobilephone', 'email']
        # headers to match - 
        # 'date_hpv_10', 'result_hpv_10', 'result_hpv18_10', 'result_hpv16_10', 'result_hpv_othr_10', 'date_cyto_10', 'result_cyto_10', 'triage_10', 'date_colpo_10', 'date_leep_10'
        for i in range(constants.EventConstants.MAX_WIDE_PATHWAYS):
            t = 'date_hpv_' + str(i + 1).zfill(2)
            r.append(t)
            t = 'hpvdna_result_' + str(i + 1).zfill(2)
            r.append(t)
            t = 'result_hpv18_' + str(i + 1).zfill(2)
            r.append(t)
            t = 'result_hpv16_' + str(i + 1).zfill(2)
            r.append(t)
            t = 'result_hpv_othr_' + str(i + 1).zfill(2)
            r.append(t)

            t = 'date_cyto_' + str(i + 1).zfill(2)
            r.append(t)
            t = 'result_cyto_' + str(i + 1).zfill(2)
            r.append(t)
            t = 'triage_' + str(i + 1).zfill(2)
            r.append(t)
            t = 'date_colpo_' + str(i + 1).zfill(2)
            r.append(t)
            t = 'date_leep_' + str(i + 1).zfill(2)
            r.append(t)
        print(r)
        return r

    def make_result_pathway_dict(self):
        result = dict()
        # we must populate the dict first to make sure that the order 
        # of "list(result.values())" matches the ultimate values going
        # into the wide results file
        result['date_hpv'] = ''
        result[constants.EventConstants.HPVDNA_NAME] = ''
        result[constants.EventConstants.HPV18_NAME] = ''
        result[constants.EventConstants.HPV16_NAME] = ''
        result[constants.EventConstants.HPVOTHR_NAME] = ''
        result['date_cyto'] = ''
        result[constants.EventConstants.CYTO_NAME] = ''
        
        result[constants.EventConstants.FOLLOWUP_NAME] = ''
        result['date_leep'] = ''
        result['date_colpo'] = ''
        # print('----')
        # print(list(result.keys()))
        return result
    
    def make_one_pathway(self, mrn, data_list):
        # mrn,collection_date,event_idx,event_name,result
        # 19343,2021-06-21,200,cyto_result,NILM
        work_dict = self.make_result_pathway_dict()
        data_list_len = len(data_list)
        if data_list_len == 0:
            self.log_mrn_info(mrn, 'data_list length 0')
            return list()
        first_idx = 0
        # walk the data list looking for second entry of 'cyto_result'
        for idx, val in enumerate(data_list):
            if val[1] == 'cyto_result':
                first_idx = idx
                break
  
        if first_idx != 0:
            self.log_mrn_info(mrn, 'bad first_idx')
            del(data_list[0:first_idx])
            first_idx = 0
        entry = data_list[first_idx]
        if entry[1] != 'cyto_result':
            self.log_mrn_info(mrn, 'corrected data_list does not start with cyto_result')
            del(data_list[0:data_list_len])
            return list()
        # headers to match - 
        # 'date_hpv_10', 'result_hpv_10', 'result_hpv18_10', 'result_hpv16_10', 'result_hpv_othr_10', 'date_cyto_10', 'result_cyto_10', 'triage_10', 'date_colpo_10', 'date_leep_10'
        # find the next index of cyto_result
        next_idx = data_list_len
        for idx, val in enumerate(data_list):
            if idx > 0:
                if val[1] == 'cyto_result':
                    next_idx = idx
                    break
        for idx, val in enumerate(data_list):
            if idx == next_idx:
                break
            # print(idx, val)
            if val[1] == 'cyto_result':
                work_dict['date_cyto'] = val[0]
                work_dict[val[1]] = val[2]
            elif val[1] in ('hpvdna_result', 'hpv18', 'hpv16', 'hpv_other'):
                work_dict['date_hpv'] = val[0]
                work_dict[val[1]] = val[2]
            elif val[1] == 'colpo':
                work_dict['date_colpo'] = val[0]
            elif val[1] == 'leep':
                work_dict['date_leep'] = val[0]
            elif val[1] == constants.EventConstants.FOLLOWUP_NAME:
                work_dict[constants.EventConstants.FOLLOWUP_NAME] = val[2]
            else:
                print('foo error')

        # work_dict is full - make a result
        result = list(work_dict.values())
        # shrink the list
        del(data_list[0:next_idx])
        return result
    
    def output_wide_row(self, writer, mrn, data_list):
        row = [mrn]
        demo_data = self.mrn_facts[mrn]
        row = row + demo_data
        pathway_len = len(data_list)
        while pathway_len > 0:
            pathway = self.make_one_pathway(mrn, data_list)
            # data_length = len(data_list)
            pathway_len = len(pathway)
            if pathway_len > 1:
                # make_one_pathway() will log messages before returning and empty pathway
                row = row + pathway
                # for i in range(0, pathway_len):
                #     row = row + pathway[i]
        # write the row
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

