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
    HPV18_IDX = 310
    HPV18_NAME = 'hpv18'
    HPV18_FILE_NAME = str(HPV18_IDX) +  '_' + HPV18_NAME + '.csv'
    HPV16_IDX = 320
    HPV16_NAME = 'hpv16'
    HPV16_FILE_NAME = str(HPV16_IDX) +  '_' + HPV16_NAME + '.csv'
    HPVOTHR_IDX = 330
    HPVOTHR_NAME = 'hpv_other'
    HPVOTHR_FILE_NAME = str(HPVOTHR_IDX) +  '_' + HPVOTHR_NAME + '.csv'

    FOLLOWUP_IDX = 400
    FOLLOWUP_NAME = 'followup'
    FOLLOWUP_FILE_NAME = str(FOLLOWUP_IDX) +  '_' + FOLLOWUP_NAME + '.csv'
    COLPO_IDX = 500
    COLPO_NAME = 'colpo'
    COLPO_FILE_NAME = str(COLPO_IDX) +  '_' + COLPO_NAME + '.csv'
    COLPO_NARRATIVE_IDX = 500
    COLPO_NARRATIVE_NAME = 'colpo_narrative'
    COLPO_NARRATIVE_FILE_NAME = str(COLPO_NARRATIVE_IDX) +  '_' + COLPO_NARRATIVE_NAME + '.csv'
    LEEP_IDX = 600
    LEEP_NAME = 'leep'
    LEEP_FILE_NAME = str(LEEP_IDX) +  '_' + LEEP_NAME + '.csv'
    LEEP_NARRATIVE_IDX = 600
    LEEP_NARRATIVE_NAME = 'leep_narrative'
    LEEP_NARRATIVE_FILE_NAME = str(LEEP_IDX) +  '_' + LEEP_NARRATIVE_NAME + '.csv'
    MERGED_IDX = 800
    MERGED_FILE_NAME = str(MERGED_IDX) +  '_merged_events.csv'
    WIDE_IDX = 900
    WIDE_FILE_NAME = str(WIDE_IDX) + '_wide.csv'

    EVENT_HEADER_LIST = ['mrn','collection_date', 'event_idx', 'event_name', 'result']

    DELTA_DAYS_CYTO_HPV_SAME = 14
    MAX_WIDE_PATHWAYS = 10

