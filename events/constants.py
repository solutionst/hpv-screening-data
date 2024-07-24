# c. 2024 solutionst.com llc
# This code is licensed under the MIT license (See LICENSE for details).
class EventConstants:
    DOB_IDX = 100
    DOB_NAME = 'dob_event'
    DOB_FILE_NAME = str(DOB_IDX) + '_' + DOB_NAME + '.csv'
    CYTO_IDX = 200
    CYTO_NAME = 'cyto_result'
    CYTO_FILE_NAME = str(CYTO_IDX) +  '_' + CYTO_NAME + '.csv'
    HPVDNA_IDX = 300
    HPVDNA_NAME = 'hpvdna_result'
    HPVDNA_FILE_NAME = str(HPVDNA_IDX) +  '_' + HPVDNA_NAME + '.csv'
    FOLLOWUP_IDX = 400
    FOLLOWUP_NAME = 'followup'
    FOLLOWUP_FILE_NAME = str(FOLLOWUP_IDX) +  '_' + FOLLOWUP_NAME + '.csv'
    COLPO_IDX = 500
    COLPO_NAME = 'colpo'
    COLPO_FILE_NAME = str(COLPO_IDX) +  '_' + COLPO_NAME + '.csv'
    LEEP_IDX = 600
    LEEP_NAME = 'leep'
    LEEP_FILE_NAME = str(LEEP_IDX) +  '_' + LEEP_NAME + '.csv'
    MERGED_IDX = 800
    MERGED_FILE_NAME = str(MERGED_IDX) +  '_merged_events.csv'
    WIDE_IDX = 900
    WIDE_FILE_NAME = str(WIDE_IDX) + '_wide_2021.csv'

    MRN_MAX_COUNT = 23

    EVENT_HEADER_LIST = ['mrn','date', 'event_idx', 'event_name', 'result']
