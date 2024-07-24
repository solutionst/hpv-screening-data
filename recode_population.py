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
        # dictionary of lists to track the source_race values included
        # in each studay race
        self.race_result = dict()

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
    
    def process_population(self):
        print("summarize_results")
        line_count = 0
        with open(os.path.join(self.csv_directory, 'hpi10029_patient_demographics_2021_to_2023.csv'), 'r', encoding='utf-8') as f:
            with open (os.path.join(self.csv_directory, 'population_recoded.csv'), 'w', newline='', encoding='utf-8') as out:
                out_writer = csv.writer(out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                # mrn,PatientID,EncounterID,ACCESSION_NUMBER,ORDER_DATE,COLLECTION_DATE,OBSERVATION_DATE,ActivityDate,ORDER_CODE,ORDER_NAME,RESULT_CODE,RESULT_NAME,VALUE,RANGE,RESULT_COMMENT
                header = ('mrn', 'dob', 'deceased_date', 'study_race', 'source_race', 'ethnicity', 'lastname', 'firstname', 'middlename', 'postalcode', 'homephone', 'mobilephone', 'email')
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

                        # ('mrn', 'dob', 'deceased_date', 'study_race', 'source_race', 'ethnicity', 'lastname', 'firstname', 'middlename', 'postalcode', 'homephone', 'mobilephone', 'email')
                        result_row = (mrn, dob, deceased_date, study_race, source_race, ethnicity, lastname, firstname, middlename, postalcode, homephone, mobilephone, email)
                        out_writer.writerow(result_row)

            print(f'Processed {line_count} screened lines.')    

    def main(self):
        print("main")
        print(f'csv_directory: {self.csv_directory}')
        self.process_population()

if __name__ == '__main__':
    filter = Filter()
    filter.main()
