import math
import numpy as np
import pandas as pd
import snsql
from snsql import Privacy
import time
import matplotlib.pyplot as plt

df = pd.read_csv("Police_rank_table.csv")
df_copy = pd.read_csv("Police_rank_table.csv")
df = df.rename(columns={"rank": "police_rank"})
df = df.rename(columns={"Unnamed: 0": "id"})

location_keep = ['POLICE OFFICER', 'SERGEANT OF POLICE', 'PO AS DETECTIVE']
print(df)

df_police = df_copy.groupby("rank")['current_complaint_category'].count().rename_axis(["police_rank"])
df_police = df_police.nlargest(4)
print(df_police)

# print(df)

meta_path = {
  '':{
    'public': {
      'df': {
      	'censor_dims': False,
        'max_ids': 1,
        'row_privacy': False,
        'clamp_counts': True,
        'id': {
          'type': 'int',
          'private_id': True
        },
				'police_rank': {
          'type': 'string'
        },
        'current_complaint_category': {
          'type': 'string'
        },
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


epsilon = [0.005, 0.008, 0.01, 0.05, 0.075, 0.1, 0.4, 0.75, 1.0, 2.0]
delta = 0
epsilon2 = list(map(str, epsilon))

original_val = df_police.loc["LIEUTENANT OF POLICE"]
print(original_val)

for i in range(10):
	start_time = []
	end_time = []
	error = []
	for val in epsilon:
		privacy = Privacy(epsilon=val, delta=delta)
		reader = snsql.from_df(df, privacy=privacy, metadata=meta_path)
		start_time.append(time.time())
		result = reader.execute('SELECT police_rank, COUNT(*) AS Total FROM public.df GROUP BY police_rank')
		end_time.append(time.time())
		df_new = pd.DataFrame(result, columns = ['police_rank', 'Total'])
		df_new = df_new.iloc[1:]
		# print(df_new)
		new_val = df_new.loc[df_new['police_rank'] == "LIEUTENANT OF POLICE"].iloc[0][1]
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





