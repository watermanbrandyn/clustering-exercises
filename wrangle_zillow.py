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
    This function takes in our acquired zillow dataframe, prepares and cleans the data and returns train, validate, and test dataframe splits. 
    The main preparation is handling of nulls, eliminating unnecessary columns, and encoding the categorical columns. 
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
    df.unitcnt = df.unitcnt.fillna(value=1)
    # Dropping columns and rows that do not meet 50% threshold of non-nulls
    df = handle_missing_values(df, .5, .5)
    # Rows to drop bc they are not useful, redundant, or cause leakage
    to_drop = ['id', 'parcelid', 'calculatedbathnbr', 'finishedsquarefeet12', 'lotsizesquarefeet', 'propertycountylandusecode',
          'propertylandusetypeid', 'propertyzoningdesc', 'rawcensustractandblock', 'structuretaxvaluedollarcnt', 'taxvaluedollarcnt',
          'assessmentyear', 'landtaxvaluedollarcnt', 'taxamount', 'censustractandblock', 'transactiondate', 'heatingorsystemdesc', 
          'propertylandusedesc', 'buildingqualitytypeid', 'heatingorsystemtypeid', 'regionidcity', 'roomcnt', 'fullbathcnt', 'regionidcounty']
    # Dropping the rows
    df = df.drop(columns=to_drop)
    # Dropping data that exceeds single unit, then dropping unitcnt column
    df = df[df.unitcnt <= 1]
    df = df.drop(columns='unitcnt')
    # Dropping any nulls left (~ .1% of the rows of data dropped)
    df = df.dropna()
    # Changing yearbuilt to new feature age and dropping yearbuilt column
    df.yearbuilt = df.yearbuilt.astype(int)
    df['age'] = 2017 - df.yearbuilt
    df = df.drop(columns='yearbuilt')
    # Changing values for taxdelinquencyflag
    df.taxdelinquencyflag = np.where(df.taxdelinquencyflag == 'Y', 1, 0)
    # Dropping columns that have 0 bedrooms or bathrooms, or square feet <= 400
    df = df[(df.bathroomcnt > 0) & (df.bedroomcnt > 0) & (df.calculatedfinishedsquarefeet > 400)]
    # Fips has to be converted to int, str, and concat '0' to properly format number code
    df.fips = df.fips.astype(int)
    df.fips = df.fips.astype(str)
    df.fips = '0' + df.fips
    # Remove outliers based on bathrooms, bedrooms, square feet, and age of house
    df = remove_outliers(df, 1.5, ['bathroomcnt', 'bedroomcnt', 'calculatedfinishedsquarefeet', 'age'])
     # Changing columns to more int data type prior to change to object type (to lose the trailing 0)
    df.poolcnt = df.poolcnt.astype(int)
    df.hashottuborspa = df.hashottuborspa.astype(int)
    df.regionidzip = df.regionidzip.astype(int)
    # Changing object columns to right data type
    object_cols = ['fips', 'hashottuborspa', 'poolcnt', 'taxdelinquencyflag', 'regionidzip']
    for col in object_cols:
        df[col] = df[col].astype(object)
    # Encoding categorical columns (except regionidzip)
    encode_cols = ['fips', 'hashottuborspa', 'poolcnt', 'taxdelinquencyflag']
    for col in encode_cols:
        df = pd.get_dummies(df, columns=[col])
    # Dropping the redundant encoded columns
    df = df.drop(columns=['hashottuborspa_0', 'poolcnt_0', 'taxdelinquencyflag_0'])
    # Renaming our columns
    df = df.rename(columns={'bathroomcnt': 'bathrooms',
                       'bedroomcnt': 'bedrooms', 'calculatedfinishedsquarefeet': 'squarefeet', 'fireplacecnt': 'num_fireplace',
                       'threequarterbathnbr': 'threequarter_baths', 'hashottuborspa_1': 'hottub_or_spa', 'poolcnt_1': 'has_pool',
                       'taxdelinquencyflag_1': 'tax_delinquency'})
    # Splitting our data
    train, validate, test = zillow_split(df)
    # Return our three dataframes (train, validate, test splits)
    return train, validate, test


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


def zillow_split(df):
    '''
    This function takes in a dataframe and returns train, validate, test splits. (dataframes)
    An initial 20% of data is split to place as 'test'.
    A second split is performed, on the remaining 80% of original df, to split 70/30 between train and validate. 
    '''
    # First split with 20% going to test
    train_validate, test = train_test_split(df, train_size = .8,
                                            random_state=123)
    # Second split with 70% of remainder going to train, 30% to validate
    train, validate = train_test_split(train_validate, train_size = .7,
                                            random_state=123)
    # Return train, validate, test (56%, 24%, 20% splits of original df)
    return train, validate, test