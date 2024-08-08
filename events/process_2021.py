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
        
        self.log_file_name = os.path.join(self.out_directory, 'log_2021.csv')
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

        self.screen_file_10 = os.path.join(self.out_directory, 'screen_10.csv')
        self.screen_file_20 = os.path.join(self.out_directory, 'screen_20.csv')

        # key = mrn data is list of type and text
        # captures messages and is post-processed for later
        self.mrn_message_dict = dict()

        # set up the logger
        self.log_create()

    def determine_cyto_result(self, mrn, cyto_value_dict):
        assert len (cyto_value_dict) <= 1
        cyto = 'cyto_unknown'
        if len(cyto_value_dict) == 0:
            cyto = 'no_cyto'
            return cyto
        # get the accession ID for the single key in the dictionary
        cyto_accession = list(cyto_value_dict.keys())[0]
        work_dict = cyto_value_dict[cyto_accession]
        
        # if len(work_dict) == 1 and 'RECEIVED' in work_dict.keys[0].upper():
        if len(work_dict) == 1 and 'RECEIVED' in list(work_dict.values())[0].upper():
             cyto = 'NotReported'
        
        # transformation zone
        t_zone_present = True
        if 'ADEQ' in work_dict.keys():
            adeq_val = work_dict['ADEQ']
            if 'zone absent' in adeq_val:
                adeq_val = False
        if 'INTER' in work_dict.keys():
            val = work_dict['INTER']
            if 'ASC-H' in val:
                cyto = 'ASCH'
            elif 'ASC-US' in val:
                cyto = 'ASCUS'
            elif 'Low grade squamous intraepithelial lesion' in val:
                cyto = 'LSIL'
            elif 'Negative for intraepithelial lesion or malignancy' in val:
                cyto = 'NILM' if t_zone_present else 'NILM-NOTZ'
            elif 'for neoplastic cells: Negative' in val:
                cyto = 'NILM' if t_zone_present else 'NILM-NOTZ'
            elif 'for neoplastic cells:  Negative.' in val:
                cyto = 'NILM' if t_zone_present else 'NILM-NOTZ'
            elif 'Atypical glandular cells' in val:
                cyto = 'AGC'
            elif 'Atypical endometrial cells present' in val:
                cyto = 'AGC'
            elif 'Atypical endocervical cells present' in val:
                cyto = 'AGC'
            elif 'High grade squamous intraepithelial lesion' in val:
                cyto = 'HSIL'
            elif 'Squamous epithelial atrophy' in val:
                cyto = 'NILM' if t_zone_present else 'NILM-NOTZ'
            elif 'No malignant cells identified' in val:
                cyto = 'NILM' if t_zone_present else 'NILM-NOTZ'
            elif 'Satisfactory for evaluation. Transformation zone present' in val:
                cyto = 'NILM'
            elif 'Unsatisfactory' in val:
                cyto = 'Unsat'
        if cyto == 'cyto_unknown':
            self.log_mrn_error(mrn, 'cyto_unknown' + self.make_dict_comment(cyto_value_dict))
        return cyto
    
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
        else:
            self.log_mrn_info(mrn, 'No HPV results present')

        # screening result
        # age under 30
        if age < 30:
            if cyto == 'AGC':
                screen = 'High'
            elif cyto == 'ASCH':
                screen = 'High'
            elif cyto == 'ASCUS' and hpv == 'Pos':
                screen = 'High'
            elif cyto == 'ASCUS' and hpv == 'Neg':
                screen = 'Normal-3'
            elif cyto == 'ASCUS' and hpv == '':
                screen = 'Low-1'
            elif cyto == 'HSIL':
                screen = 'High'
            elif cyto == 'LSIL' and (hpv16 == 'Pos' or hpv18 == 'Pos' or hpv_other == 'Pos'):
                screen = 'High'
            elif cyto.startswith('NILM') and (hpv16 == 'Pos' or hpv18 == 'Pos'):
                screen = 'High'
            elif cyto == 'Unsat' and (hpv16 == 'Pos' or hpv18 == 'Pos'):
                screen = 'High'
            elif cyto == 'Unsat' and hpv == 'Neg':
                # needs spreadsheet update
                screen = 'Censored'
            elif cyto == 'Unsat' and hpv == '':
                # needs spreadsheet update
                screen = 'Censored'
            elif cyto == 'NILM' and (hpv == 'Pos' or hpv_other == 'Pos'):
                screen = 'High'
            elif cyto == 'LSIL':
                screen = 'Low-1'
            elif cyto == 'NILM-NOTZ' and hpv == 'Neg':
                screen = 'Normal-3'
            elif cyto == 'NILM-NOTZ' and hpv == '':
                screen = 'Normal-3'
            elif cyto == 'NILM-NOTZ' and hpv == '':
                screen = 'Low-1'
            elif cyto == 'NILM':
                screen = 'Normal-3'
            elif cyto == 'no_cyto' and hpv == 'Neg':
                # needs spreadsheet update
                screen = 'Censored'
            else:
                # needs spreadsheet update
                screen = 'Censored'
                self.log_mrn_info(mrn, 'Under 30 - Other')
        else:
            if cyto == 'ASCUS' and hpv == '':
                screen = 'Censored'
            elif cyto == 'NILM' and hpv == '':
                screen = 'Censored'
            elif cyto == 'AGC':
                screen = 'High'
            elif cyto == 'ASCH':
                screen = 'High'
            elif cyto == 'ASCUS' and hpv == 'Pos':
                screen = 'High'
            elif cyto == 'HSIL':
                screen = 'High'
            elif cyto == 'LSIL' and hpv == 'Neg':
                screen = 'Low-1'
            elif cyto == 'LSIL':
                screen = 'High'
            elif cyto.startswith('NILM') and hpv == 'Pos':
                screen = 'High'
            elif cyto == 'Unsat' and (hpv16 == 'Pos' or hpv18 == 'Pos'):
                screen = 'High'
            elif cyto == 'Unsat' and hpv == 'Pos':
                screen = 'Censored'
            elif cyto == 'Unsat' and hpv == 'Neg':
                # needs spreadsheet update
                screen = 'Normal-5'
            elif cyto == 'Unsat' and hpv == '':
                # needs spreadsheet update
                screen = 'Censored'
            elif cyto.startswith('NILM') and hpv == 'Neg':
                screen = 'Normal-5'
            elif cyto == 'ASCUS' and hpv == 'Neg':
                screen = 'Normal-5'
            elif cyto == 'no_cyto' and hpv == 'Neg':
                # needs spreadsheet update
                screen = 'Normal-5'
            else:
                # needs spreadsheet update
                screen = 'Censored'
                self.log_mrn_info(mrn, '30 and up - Other')

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
    worker = Process2021()
    worker.main()
