# c. 2024 solutionst.com llc
# This code is licensed under the MIT license (See LICENSE for details).
import os
import sys
import pandas as pd
import csv


DATETIME_FMT = '%m/%d/%Y %H:%M'
DATE_FMT = '%m/%d/%Y'
DATE_FMT_PD = '%Y-%m-%d'
SCRIPT_DIRECTORY = os.path.dirname(os.path.abspath(sys.argv[0]))

class Events(object):
    
    def __init__(self):
        print ('Event.__init__')
    
    @staticmethod
    def load_data_to_df(in_file_name):
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
        df_raw['COLLECTION_DATE'] = pd.to_datetime(df_raw['COLLECTION_DATE'], format=DATETIME_FMT).dt.date
        df_raw = df_raw.sort_values(['mrn', 'COLLECTION_DATE'], ascending=[True, True])
        return df_raw

    @staticmethod
    def print_summary(df):
        print('Summary:')
        print(df.shape)
        print(df.head())

    @staticmethod
    def save_temp_to_csv(df, file_name):
        df.to_csv(file_name, index=False)

    
