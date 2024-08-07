# c. 2024 solutionst.com llc
# This code is licensed under the MIT license (See LICENSE for details).
import os
import pandas as pd
import csv
import datetime
import constants
from process_events import ProcessEvents

class Process2021(ProcessEvents):
    def __init__(self):
        print("__init__")
        # mac
        self.in_directory = os.path.join('/Volumes', 'KINGSTON', 'original','2021-2023')
        self.out_directory = os.path.join('/Volumes', 'KINGSTON', 'out', '2021')
        # windows
        # self.in_directory = os.path.join('d:','original','2021-2023')
        # self.out_directory = os.path.join('d:', 'out', '2021')
        
        self.in_file_name = os.path.join(self.in_directory, 'hpi10029_cervical_and_Cyto_labresults_2021_to_2023.csv')
        self.demo_mrn = 2
        self.demo_lastname = 3
        self.demo_firstname = 4
        self.demo_middlename = 5
        self.demo_dob = 6
        self.demo_deceased_date = 7
        self.demo_source_race = 11
        self.demo_ethnicity = 12
        self.demo_postalcode = 19
        self.demo_homephone = 20
        self.demo_mobilephone = 21
        self.demo_email = 22
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

        self.mrn_max_count = 0
        
        self.mrn_facts = dict()
        self.screening_mrn = set()

        self.temp_file_name = os.path.join(self.out_directory, 'temp_screen.csv')
        self.temp_detail_name = os.path.join(self.out_directory, 'temp_screen_details.csv')

    
    def determine_results(self, age, cyto_value_list, hpv_value_dict):
        cyto = 'Unknown'
        hpv = ''
        hpv_other = ''
        hpv16 = ''
        hpv18 = ''
        screen = 'Screen_Unknown'
        comment = ''
        # cytology encoding
        for val in cyto_value_list:
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
            comment = "#".join(cyto_value_list)
        # hpv encoding
        if len(hpv_value_dict) > 0:
            hpv_other = self.code_hpv_value('HPVOHR', hpv_value_dict)
            hpv16 = self.code_hpv_value('HPV16', hpv_value_dict)
            hpv18 = self.code_hpv_value('HPV18', hpv_value_dict)
            if 'HPVR' in hpv_value_dict:
                work = hpv_value_dict['HPVR']
                if work is None:
                    hpv = ''
                elif 'High Risk Human Papillomavirus detected' in work:
                    hpv = 'Pos'
                elif 'High Risk Human Papillomavirus not detected' in work:
                    hpv = 'Neg'
                else:
                    hpv = ''
            else:
                hpv = ''
            if hpv_other == '' or hpv16 == '' or hpv18 == '' or hpv == '':
                hpv_comment = self.make_hpv_comment(hpv_value_dict)
                comment = comment + hpv_comment
        else:
            comment = comment + '#No HPV results present'

        # screening result
        # age 30 and over
        if age >= 30:
            if (hpv16 == 'Pos' or hpv18 == 'Pos'):
                screen = 'High'
            elif cyto == 'ASCH':
                screen = 'High'
            elif hpv_other == 'Pos' and 'ASC' in cyto:
                screen = 'High'
            elif hpv_other == 'Pos' and cyto != 'NILM':
                screen = 'High'
            elif hpv_other == 'Pos' and cyto == "NILM":
                screen = 'Low-1'
            elif cyto == 'ASCUS' and hpv == '':
                screen = 'Low-1'
            elif hpv == 'Neg' and cyto == 'LSIL':
                screen = 'Low-3'
            elif hpv == 'Neg' and cyto == 'ASCUS':
                screen = 'Normal-5'
            elif hpv == 'Neg' and cyto == 'NILM':
                screen = 'Normal-5'
            elif cyto == 'Unknown' and hpv == 'Neg':
                screen = 'Unsat-HPV-neg'
            elif cyto == 'NILM' and hpv == '':
                screen = 'Normal-3'
            elif cyto == 'LSIL' and hpv == '':
                screen = 'Low-1'
            elif cyto == 'Unknown' and hpv == '':
                screen = 'Unsat'
        else:
            if (hpv16 == 'Pos' or hpv18 == 'Pos'):
                screen = 'High'
            elif cyto == 'ASCH':
                screen = 'High'
            elif cyto == 'NILM' and hpv_other == 'Pos':
                screen = 'Low-1'
            elif cyto == 'NILM' and hpv == '':
                screen = 'Normal-3'
            elif cyto == 'NILM' and hpv == 'Neg':
                screen = 'Normal-3'
            elif cyto == 'Unknown' and hpv == '':
                screen = 'Unsat'
            elif cyto == 'ASCUS' and hpv == 'Neg':
                screen = 'Normal-3'
            elif cyto == 'ASCUS' and hpv == 'Pos':
                screen = 'High'
            elif cyto == 'Unknown' and hpv == 'Neg':
                screen = 'Unsat-HPV-neg'
            elif cyto == 'LSIL':
                screen = 'Low-1'
            elif cyto == 'ASCUS' and hpv == '':
                screen = 'Low-1'
            elif cyto == 'Unknown' and hpv == 'Pos':
                screen = 'Low-1'
                
        return cyto, hpv, hpv_other, hpv16, hpv18, screen, comment
 
    def main(self):
        print("main")

        self.make_mrn_facts()
        self.df = self.load_data_to_df(self.in_file_name)
        self.print_summary(self.df)
        self.save_temp_to_csv(self.df, self.temp_file_name)
        self.summarize_results()
        detail_df = self.sort_detail_and_save()
        self.create_lab_event_files(detail_df)
        self.process_followup_events(self.leep_in_file_name, self.leep_file_name, constants.EventConstants.LEEP_IDX, constants.EventConstants.LEEP_NAME)
        self.process_followup_events(self.colpo_in_file_name, self.colpo_file_name, constants.EventConstants.COLPO_IDX, constants.EventConstants.COLPO_NAME)
        self.consolidate_and_sort()
        self.create_wide_file()

        exit()

if __name__ == '__main__':
    worker = Process2021()
    worker.main()
