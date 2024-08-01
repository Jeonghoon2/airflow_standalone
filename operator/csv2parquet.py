#df = pd.read_csv('~/data/csv/20240717/csv.csv', 
#                 on_bad_lines='skip', 
#                 names=['dt', 'cmd', 'cnt'])

#df['dt'] = df['dt'].str.replace('^', '')
#df['cmd'] = df['cmd'].str.replace('^', '')
#df['cnt'] = df['cnt'].str.replace('^', '')

#df['cnt'] = df['cnt'].astype(int)

#fdf = df[df['cmd'].str.contains('aws')]

#cnt = fdf['cnt'].sum()

#print(cnt)

#df.to_parquet("~/tmp/history.parquet")

import pandas as pd
import sys

READ_PATH=sys.argv[1]
SAVE_PATH=sys.argv[2]

df = pd.read_csv(READ_PATH, encoding='latin', on_bad_lines='skip', names = ['dt', 'cmd', 'cnt'])
df['dt'] = df['dt'].str.replace('^', '')
df['cmd'] = df['cmd'].str.replace('^', '')
df['cnt'] = df['cnt'].str.replace('^', '')
df['cnt']=pd.to_numeric(df['cnt'], errors='coerce')
fdf = df[df['cmd'].str.contains(sys.argv[1])]
cnt = fdf['cnt'].fillna(0).astype(int).sum()
df.to_parquet(f'{SAVE_PATH}', partition_cols=['dt'])
print(cnt)
