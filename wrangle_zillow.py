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


def prepare_zillow(df):
    '''
    
    '''
    # Removal of data that does not meet our requirement of being single unit or family homes
    # Based on codes for 'propertylandusetypeid' column
    not_single = [246, 248, 247, 267, 31]
    # Removal from df
    df = df[~df.propertylandusetypeid.isin(not_single)]
    # Changing nulls in columns where deemed appropriate
    cols = ['fireplacecnt', 'hashottuborspa', 'poolcnt', 'threequarterbathnbr', 'taxdelinquencyflag']
    for col in cols:
        df[col] = df[col].fillna(value=0)
    # Dropping columns and rows that do not meet 50% threshold of non-nulls
    df = handle_missing_values(df, .5, .5)
    # Rows to drop bc they are not useful, redundant, or cause leakage
    to_drop = ['id', 'parcelid', 'calculatedbathnbr', 'finishedsquarefeet12', 'lotsizesquarefeet', 'propertycountylandusecode',
          'propertylandusetypeid', 'propertyzoningdesc', 'rawcensustractandblock', 'structuretaxvaluedollarcnt', 'taxvaluedollarcnt',
          'assessmentyear', 'landtaxvaluedollarcnt', 'taxamount', 'censustractandblock', 'transactiondate', 'heatingorsystemdesc', 
          'propertylandusedesc', 'buildingqualitytypeid', 'heatingorsystemtypeid', 'regionidcity', 'roomcnt', 'fullbathcnt']
    # Dropping the rows
    df = df.drop(columns=to_drop)
    # Dropping data that exceeds single unit, then dropping unitcnt column
    df = df[df.unitcnt <= 1]
    df = df.drop(columns='unitcnt')
    # Dropping any nulls left (~ .1% of the rows of data dropped)
    df = df.dropna()
    # Changing yearbuilt to new feature age and dropping yearbuilt column
    df.yearbuilt = df.yearbuilt.astype(int)
    df['age'] = 2017 - df
    df = df.drop(columns='yearbuilt')
    # Dropping columns that have 0 bedrooms or bathrooms, or square feet <= 400
    df = df[(df.bathroomcnt > 0) & (df.bedroomcnt > 0) & (df.calculatedfinishedsquarefeet > 400)]
    # Fips has to be converted to int, str, and concat '0' to properly format number code
    df.fips = df.fips.astype(int)
    df.fips = df.fips.astype(str)
    df.fips = '0' + df.fips




def handle_missing_values(df, prop_required_column, prop_required_row):
    '''
    This function takes in a dataframe and the required proportion of non-nulls for column and row. It returns the dataframe after
    dropping the columns and rows that do not meet the required proportion.
    '''
    # Specifying the number of columns and rows needed to meet threshold
    n_required_column = round(df.shape[0] * prop_required_column)
    n_required_row = round(df.shape[1] * prop_required_row)
    # Dropping the columns and rows that do not meet the thresh value
    df = df.dropna(axis=0, thresh=n_required_row)
    df = df.dropna(axis=1, thresh=n_required_column)
    # Return the dataframe
    return df


def remove_outliers(df, k, cols):
    '''
    This function takes in a dataframe, a specified 'k' value (how sensitive to make outlier detection), and a list of 
    columns to remove outliers from. It returns the dataframe without the outliers.
    '''
    # Cycle through specified cols
    for col in cols:
        # Determine the quartiles for each column
        q1, q3 = df[col].quantile([.25, .75])
        # Compute the interquartile range
        iqr = q3 - q1
        # Calculate the upper and lower bounds
        upper_bound = q3 + k * iqr
        lower_bound = q1 - k * iqr
        # Remove the outliers
        df = df[(df[col] > lower_bound) & (df[col] < upper_bound)]
        # Return the dataframe without outliers
    return df