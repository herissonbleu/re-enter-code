import pandas as pd
import numpy as np

"""Lirumlarumopsum"""

path = pd.read_csv(r'filepath',
    low_memory=False, dtype=object)
data=pd.DataFrame(path)
data['entry_time'] = pd.to_datetime(data['entry_time'],errors='coerce')
data['exit_time'] = pd.to_datetime(data['exit_time'],errors='coerce')
data['client_id'] = pd.to_numeric(data['client_id'],errors='coerce')
df = data[data.duplicated(subset='client_id', keep=False)]
df = data.sort_values(by='client_id')
#print(df.head(10))

def re_enter_within_7days(df, adm):
    df_of_adm = df[df['client_id']==adm].sort_values(by = 'entry_time')
    t1 = df_of_adm['entry_time'].reset_index(drop = True)[0]
    df_of_adm_exit = df[df['client_id']==adm].sort_values(by = 'entry_time')
    df_of_adm2 = df[df['client_id']==adm].sort_values(by = 'exit_time')
    t2 = df_of_adm2['exit_time'].reset_index(drop = True)[0]
    datetime_deltas = [(i - t1).total_seconds() for i in df_of_adm['entry_time']]
    datetime_exit_deltas = [(i -t2).total_seconds() for i in df_of_adm2['entry_time']]
    filter_list = [1800 < i <= 604800.0 and  j > 0 for i,j in zip(datetime_deltas,datetime_exit_deltas)]
    return df_of_adm[filter_list]

adm = data['client_id'].unique()
new_df = pd.concat([re_enter_within_7days(data,i) for i in adm])

new_df['count'] = new_df.groupby('client_id').count()
data_file = pd.concat([data,new_df], axis=1)

print('New DF: ',data_file.head(5))
new_df = new_df.sort_values('client_id')
print('Total entries: ',data[data.columns[0]].count())
print('Sum of re-entries inside 7 days: ', new_df[new_df.columns[0]].count())
print('Sum of clients: ',data['client_id'].nunique())
duplicates_in_clients = data.duplicated(subset='client_id', keep='first').sum()
print('Amount of clients who have attended minimum 2 times: ',duplicates_in_clients)
print('Amount of clients who had minimum one re-entry inside 7 days', new_df['client_id'].unique().size)
#data_file.to_csv(r'filepath',index=False)
