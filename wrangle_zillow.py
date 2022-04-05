import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler

from env import user, host, password


#---------------Acquisition

def get_db_url(db_name, username=user, hostname=host, password=password):
    '''
    This function requires a database name (db_name) and uses the imported username,
    hostname, and password from an env file.
    A url string is returned using the format required to connect to a SQL server.
    '''
    url = f'mysql+pymysql://{username}:{password}@{host}/{db_name}'
    return url


# The query needed to acquire the desired zillow data from the SQL server
# Currently creates duplicates on parcelid
query = '''
SELECT  prop.*,
        predictions_2017.logerror, 
        predictions_2017.transactiondate,
        air_cond.airconditioningdesc,
        architecture.architecturalstyledesc,
        building_class.buildingclassdesc,
        heating.heatingorsystemdesc,
        landuse.propertylandusedesc,
        story.storydesc,
        construct.typeconstructiondesc

FROM properties_2017 prop
    JOIN (
        SELECT parcelid, Max(transactiondate) AS max_transactiondate
            FROM predictions_2017
            GROUP BY parcelid) pred
            USING (parcelid)
JOIN predictions_2017 ON pred.parcelid = predictions_2017.parcelid
                      AND pred.max_transactiondate = predictions_2017.transactiondate
LEFT JOIN airconditioningtype air_cond USING (airconditioningtypeid)
LEFT JOIN architecturalstyletype architecture USING (architecturalstyletypeid)
LEFT JOIN buildingclasstype building_class USING (buildingclasstypeid)
LEFT JOIN heatingorsystemtype heating USING (heatingorsystemtypeid)
LEFT JOIN propertylandusetype landuse USING (propertylandusetypeid)
LEFT JOIN storytype story USING (storytypeid)
LEFT JOIN typeconstructiontype construct USING (typeconstructiontypeid)
WHERE prop.latitude IS NOT NULL
        AND prop.longitude IS NOT NULL
        AND transactiondate <= '2017-12-31'
'''


# Acquire zillow data
def acquire_zillow(use_cache = True):
    '''
    This function uses our above query to interact with SQL server and obtain zillow data (acquire)
    '''
    # Checking to see if data already exists in local csv file
    if os.path.exists('zillow.csv') and use_cache:
        print('Using cached csv')
        return pd.read_csv('zillow.csv')
    # If data is not local we will acquire it from SQL server
    zillow_df = pd.read_sql(query, get_db_url('zillow'))
    # Creating csv file
    zillow_df.to_csv('zillow.csv', index=False)
    # Return the df
    return zillow_df


def nulls_by_col(df):
    '''
    This function takes in a dataframe and returns a dataframe consisting of a count of nulls and what percent of rows are null. 
    This is done for each column in the original df. (df.column becomes the index)
    '''
    nulls = pd.DataFrame({
        'count_nulls': df.isna().sum(),
        'pct_rows_null': df.isna().mean()
    })
    return nulls


def nulls_by_rows(df):
    '''
    This function takes in a dataframe and returns a dataframe consisting of a count of how many columns are missing,
    how many rows are missing that number of columns, and what percent of columns are missing. 
    '''
    df2 = pd.DataFrame(df.isnull().sum(axis=1), columns = ['num_cols_missing']).reset_index()\
    .groupby('num_cols_missing').count().reset_index().\
    rename(columns = {'index': 'num_rows'})
    df2['pct_cols_missing'] = df2.num_cols_missing/df.shape[1]
    return df2


#---------------Preparation


