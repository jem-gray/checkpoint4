import math
import numpy as np
import pandas as pd
import snsql
from snsql import Privacy
import time
import matplotlib.pyplot as plt

complaints = pd.read_csv("complaints-complaints_2000-2018_2018-03.csv")
victims = pd.read_csv("complaints-victims_2000-2018_2018-03.csv")

df = pd.merge(victims, complaints, on="cr_id")

# drop unncessary columns
df_2 = df.drop(columns= ['row_id_x', 'complaint_date_x', 'investigating_agency_x',
	   'complainant_type_x', 'complainant_subtype', 'current_complaint_category_code',
       'injured', 'injury_condition', 'injury_description',
       'row_id_y', 'incident_start_date', 'incident_end_date',
       'complaint_date_y', 'closed_date', 'current_complaint_status',
       'incident_location', 'address_number', 'street_direction',
       'street_name', 'apartment_no', 'city', 'state', 'zip', 'incident_beat', 'was_police_shooting',
       'complainant_type_y', 'investigating_agency_y', 'current_complaint_category_type'
       , 'birth_year'])

# print(df_2.dtypes)
df_2 = df_2.dropna(how='any', axis=0)
complaint_category_keep = ['EXCESSIVE FORCE / ON DUTY - INJURY', 'INADEQUATE / FAILURE TO PROVIDE SERVICE']
df_2 = df_2.loc[df_2.current_complaint_category.isin(complaint_category_keep) == True]

df_race = df_2.groupby(['current_complaint_category', 'race'], as_index=False).count()
print(df_race)
print(df_2.columns)

meta_path = {
  '':{
    'public': {
      'df_2': {
      	'censor_dims': False,
        'max_ids': 1,
        'row_privacy': False,
        'clamp_counts': True,
        'cr_id': {
          'type': 'int',
          'private_id': True
        },
        'race': {
          'type': 'string'
        },
        'gender': {
          'type': 'string'
        },
        'current_complaint_category': {
          'type': 'string'
        }
      }
    }
  }
}

epsilon = [0.0005, 0.005, 0.0075, 0.01, 0.05, 0.075, 0.1, 0.5, 0.75, 1.0]
delta = 0
epsilon2 = list(map(str, epsilon))

# graph error rates for BLACK, Failure to provide service
original_val = df_race.iloc[2][2]
print(original_val)

for i in range(10):
	start_time = []
	end_time = []
	error = []
	for val in epsilon:
		privacy = Privacy(epsilon=val, delta=delta)
		reader = snsql.from_df(df_2, privacy=privacy, metadata=meta_path)
		start_time.append(time.time())
		result = reader.execute('SELECT current_complaint_category, race, COUNT(*) AS num_complaints FROM public.df_2 GROUP BY race, current_complaint_category')
		end_time.append(time.time())
		df_new = pd.DataFrame(result, columns =['current_complaint_category', 'race', 'num_complaints'])
		df_new = df_new.iloc[1:]
		new_val = df_new.iloc[3][2]
		error_val = (original_val - new_val) / original_val
		error.append(abs(error_val))
		print(new_val, abs(error_val))

	runtime = []
	for item1, item2 in zip(end_time, start_time):
	    item = item1 - item2
	    runtime.append(round(item, 3))

	plt.figure(0)
	plt.title("Privacy Budget vs Runtime")
	plt.plot(epsilon2, runtime, 'ro', color='black')

	plt.figure(1)
	plt.title("Privacy Budget vs Error")
	plt.plot(epsilon2, error, 'ro', color='black')

plt.show()







