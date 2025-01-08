# c. 2024 solutionst.com llc
# This code is licensed under the MIT license (See LICENSE for details).
import os
import sys
import pandas as pd
import csv
import datetime
import constants
from process_events import ProcessEvents

class Process2013(ProcessEvents):
    def __init__(self):
        print("__init__")
        self.script_directory = os.path.dirname(os.path.abspath(sys.argv[0]))
        # mac
        self.in_directory = os.path.join('/Volumes', 'KINGSTON', '2013-2019', 'input')
        self.out_directory = os.path.join('/Volumes', 'KINGSTON', '2013-2019', 'output')
        # windows
        # self.in_directory = os.path.join('d:','original','2021-2023')
        # self.out_directory = os.path.join('d:', 'out', '2021')
        if not os.path.exists(self.out_directory):
            os.makedirs(self.out_directory)

        self.log_file_name = os.path.join(self.out_directory, 'log_2013.csv')
        self.in_file_name = os.path.join(self.in_directory, 'hpi10029_cervical_and_cyto_labresults_06052024.csv')
        self.out_file_name = os.path.join(self.out_directory, 'screen_details.csv')
        self.demographic_file_name = os.path.join(self.in_directory, 'hpi10029_Patient_demographics_2013_to_2019.csv')
        self.demo_mrn = 1
        self.demo_lastname = 2
        self.demo_firstname = 3
        self.demo_middlename = 4
        self.demo_dob = 5
        self.demo_deceased_date = 6
        self.demo_source_race = 10
        self.demo_ethnicity = 11
        self.demo_postalcode = 18
        self.demo_homephone = 19
        self.demo_mobilephone = 20
        self.demo_email = 21

        self.cyto_file_name = os.path.join(self.out_directory, constants.EventConstants.CYTO_FILE_NAME)
        self.hpvdna_file_name = os.path.join(self.out_directory, constants.EventConstants.HPVDNA_FILE_NAME)
        self.hpv18_file_name = os.path.join(self.out_directory, constants.EventConstants.HPV18_FILE_NAME)
        self.hpv16_file_name = os.path.join(self.out_directory, constants.EventConstants.HPV16_FILE_NAME)
        self.hpvothr_file_name = os.path.join(self.out_directory, constants.EventConstants.HPVOTHR_FILE_NAME)
        self.followup_file_name = os.path.join(self.out_directory, constants.EventConstants.FOLLOWUP_FILE_NAME)
        self.colpo_file_name = os.path.join(self.out_directory, constants.EventConstants.COLPO_FILE_NAME)
        self.leep_file_name = os.path.join(self.out_directory, constants.EventConstants.LEEP_FILE_NAME)
        self.dob_file_name  = os.path.join(self.out_directory, constants.EventConstants.DOB_FILE_NAME)
        self.merged_events_name = os.path.join(self.out_directory, constants.EventConstants.MERGED_FILE_NAME)
        self.wide_file_name = os.path.join(self.out_directory, constants.EventConstants.WIDE_FILE_NAME)


        self.leep_in_file_name = os.path.join(self.in_directory, 'hpi10029_LEEP_labresults.csv')
        self.colpo_in_file_name = os.path.join(self.in_directory, 'hpi10029_colpo_labresults.csv')

        # self.datetime_fmt = '%m/%d/%Y %H:%M'
        # self.date_fmt = '%m/%d/%Y'
        # self.date_fmt_pd = '%Y-%m-%d'
        
        self.mrn_facts = dict()
        self.screening_mrn = set()

        self.screen_file_10 = os.path.join(self.out_directory, 'screen_10.csv')
        self.screen_file_20 = os.path.join(self.out_directory, 'screen_20.csv')

        #  Order of severity
        self.cyto_severity = ['HSIL', 'ASCH', 'AGC', 'LSIL', 'ASCUS', 'NILM-NOTZ', 'NILM', 'Unsat', 'None', 'NotReported', 'cyto_unknown']
        # set up the logger
        self.log_create()

    def determine_results(self, mrn, age, cyto_value_dict, hpv_value_dict):
        assert len(hpv_value_dict) <= 1
        cyto = self.determine_cyto_result(mrn, cyto_value_dict)
        hpv = ''
        hpv_other = ''
        hpv16 = ''
        hpv18 = ''
        screen = 'Screen_Unknown'
        comment = ''
        
        if cyto == 'cyto_unknown':
            comment = self.make_dict_comment(cyto_value_dict)
        # hpv encoding
        if len(hpv_value_dict) > 0:
            hpv_other = self.code_hpv_value(mrn, 'HPVOHR', hpv_value_dict)
            hpv16 = self.code_hpv_value(mrn, 'HPV16', hpv_value_dict)
            hpv18 = self.code_hpv_value(mrn, 'HPV18', hpv_value_dict)
            hpv = self.code_hpv_result(mrn, hpv_value_dict)
            if hpv_other == '' or hpv16 == '' or hpv18 == '' or hpv == '':
                hpv_comment = self.make_dict_comment(hpv_value_dict)
                comment = comment + hpv_comment

        # screening result
        # under 30
        if age < 30:
            if cyto == 'AGC':
                screen = 'High'
            elif cyto == 'ASCH':
                screen = 'High'
            elif cyto == 'HSIL':
                screen = 'High'
            elif cyto == 'ASCUS' and hpv == 'Pos':
                screen = 'High'
            elif cyto == 'ASCUS' and hpv == 'Neg':
                screen = 'Normal-3'
            elif cyto == 'ASCUS' and hpv == '':
                screen = 'Low-1'
            elif cyto == 'LSIL' and hpv == '':
                screen = 'Low-1'
            elif cyto == 'LSIL' and hpv == 'Pos':
                screen = 'Low-1'
            elif cyto == 'LSIL' and hpv == 'Neg':
                screen = 'Normal-3'
            elif cyto == 'NILM' and hpv == 'Pos':
                screen = 'Low-1'
            elif cyto == 'NILM' and hpv == 'Neg':
                screen = 'Normal-3'
            elif cyto == 'NILM' and hpv == '':
                screen = 'Normal-3'
            elif cyto == 'Unsat':
                screen = 'Censored'
            elif cyto == 'NotReported':
                screen = 'Censored'
            elif cyto == 'NoCyto':
                screen = 'Censored'
        else:
            if cyto == 'Unsat':
                screen = 'Censored'
            elif cyto == 'NotReported':
                screen = 'Censored'
            elif cyto == 'NoCyto':
                screen = 'Censored'
            elif cyto == 'AGC':
                screen = 'High'
            elif cyto == 'ASCH':
                screen = 'High'
            elif cyto == 'HSIL':
                screen = 'High'
            elif cyto == 'LSIL' and hpv == '':
                screen = 'High'
            elif cyto == 'LSIL' and hpv == 'Neg':
                screen = 'Low-1'
            elif cyto == 'LSIL' and hpv == 'Pos':
                screen = 'High'
            if cyto.startswith('NILM') and (hpv16 == 'Pos' or hpv18 == 'Pos'):
                screen = 'High'
            if cyto.startswith('NILM') and hpv == 'Pos':
                screen = 'High'
            elif cyto.startswith('NILM') and (hpv_other == 'Pos'):
                screen = 'Low-1'
            elif cyto == 'NILM' and hpv == 'Neg':
                screen = 'Normal-5'
            elif cyto == 'NILM' and hpv == '':
                screen = 'Normal-5'
            elif cyto == 'NILM-NOTZ' and hpv == 'Neg':
                screen = 'Normal-3'
            elif cyto == 'NILM-NOTZ' and hpv == '':
                screen = 'Normal-3'
            elif cyto == 'ASCUS' and hpv == 'Pos':
                screen = 'High'
            elif cyto == 'ASCUS' and hpv == 'Neg':
                screen = 'Normal-3'
            elif cyto == 'ASCUS' and hpv == '':
                screen = 'Low-1'
                
        return cyto, hpv, hpv_other, hpv16, hpv18, screen, comment

    def main(self):
        print("main")

        self.make_mrn_facts()
        self.df = self.load_data_to_df(self.in_file_name)
        self.print_summary(self.df)
        self.save_temp_to_csv(self.df, self.screen_file_10)
        self.summarize_results()
        detail_df = self.sort_detail_and_save()
        self.create_lab_event_files(detail_df)
        self.process_followup_events(self.leep_in_file_name, self.leep_file_name, constants.EventConstants.LEEP_IDX, constants.EventConstants.LEEP_NAME)
        self.process_followup_events(self.colpo_in_file_name, self.colpo_file_name, constants.EventConstants.COLPO_IDX, constants.EventConstants.COLPO_NAME)
        self.consolidate_and_sort()
        self.create_wide_file()
        
        exit()

if __name__ == '__main__':
    worker = Process2013()
    worker.main()
