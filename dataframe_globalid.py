import pandas as pd
import uuid
global_id_tag_name="ecommerce"
dataframe_one=pd.read_csv('ecomm_mobile_email_0.csv',nrows=20000)
dataframe_two=dataframe_one.user_id
# dataframe_two.insert(loc=0, column='global_id', value='global_id')
dataframe_two['global_id'] = ''
dataframe_two['global_id'] = dataframe_two['global_id'].apply(lambda x: global_id_tag_name + str(uuid.uuid4()))
print(dataframe_two)


