import pandas as pd

df = pd.read_csv('sample_pandas_normal.csv')
print(df)
df_s = df.set_index('state')
print(df_s)
df_r = df_s.reset_index()
print(df_r)
