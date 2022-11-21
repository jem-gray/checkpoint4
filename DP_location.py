import math
import numpy as np
import pandas as pd
import snsql
from snsql import Privacy
import time
import matplotlib.pyplot as plt

df = pd.read_csv("processing_time_df.csv")
df_copy = pd.read_csv("processing_time_df.csv")
df = df.drop(columns= ['Unnamed: 0'])
df_copy = df_copy.drop(columns= ['Unnamed: 0'])

location_keep = ['STREET', 'RESIDENCE', 'POLICE FACILITY/VEH PARKING LOT']
print(df)

df_location = df_copy.groupby("incident_location")['cr_id'].count().rename_axis(["Location Type"])
df_location = df_location.nlargest(4)
print(df_location)

# print(df)

meta_path = {
  '':{
    'public': {
      'df': {
      	'censor_dims': False,
        'max_ids': 1,
        'row_privacy': False,
        'clamp_counts': True,
        'current_complaint_category': {
          'type': 'string'
        },
        'days_open': {
          'type': 'int',
          'lower': 21,
          'upper': 1560
        },
        'incident_location': {
          'type': 'string'
        },
        'cr_id': {
          'type': 'int',
          'private_id': True
        }
      }
    }
  }
}

# epsilon = 1.0
# delta = 0

# privacy = Privacy(epsilon=epsilon, delta=delta)
# reader = snsql.from_df(df, privacy=privacy, metadata=meta_path)
# result = reader.execute('SELECT current_complaint_category, AVG(days_open) AS mean FROM public.df GROUP BY current_complaint_category LIMIT 10')
# print(result)


epsilon = [0.005, 0.01, 0.02, 0.05, 0.075, 0.1, 0.4, 0.75, 1.0, 2.0]
delta = 0
epsilon2 = list(map(str, epsilon))

original_val = df_location.loc["SIDEWALK"]
print(original_val)

for i in range(10):
	start_time = []
	end_time = []
	error = []
	for val in epsilon:
		privacy = Privacy(epsilon=val, delta=delta)
		reader = snsql.from_df(df, privacy=privacy, metadata=meta_path)
		start_time.append(time.time())
		result = reader.execute('SELECT incident_location, COUNT(*) AS Total FROM public.df GROUP BY incident_location')
		end_time.append(time.time())
		df_new = pd.DataFrame(result, columns = ['incident_location', 'Total'])
		df_new = df_new.iloc[1:]
		print(df_new)
		new_val = df_new.loc[df_new['incident_location'] == "SIDEWALK"].iloc[0][1]
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





