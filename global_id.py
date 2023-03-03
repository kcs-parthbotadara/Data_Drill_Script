import uuid
import pandas as pd

df = pd.read_csv('ecomm_mobile_email_0.csv', nrows=10000)
df.insert(loc=0, column='global_id', value='global_id')
df['global_id'] = df['global_id'].apply(lambda x: 'ecomm_' + str(uuid.uuid4()))
print(df)
# df.to_csv('global_id.csv', index=False)
# if 516349170 in df['user_id'].values:
#     print(df[df['user_id'] == 516349170]['global_id'].iloc[0])