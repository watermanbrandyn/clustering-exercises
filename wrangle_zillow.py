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
        pred.logerror, 
        pred.transactiondate,
        air_cond.airconditioningdesc,
        architecture.architecturalstyledesc,
        building_class.buildingclassdesc,
        heating.heatingorsystemdesc,
        landuse.propertylandusedesc,
        story.storydesc,
        construct.typeconstructiondesc

FROM properties_2017 prop
    JOIN (
        SELECT parcelid,
                Max(transactiondate) transactiondate
                    FROM predictions_2017
                    GROUP BY parcelid) pred
                    USING (parcelid)
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


#---------------Preparation

