import os
import pandas as pd




query = 
'''
SELECT *
FROM properties_2017
LEFT JOIN airconditioningtype USING (airconditioningtypeid)
LEFT JOIN architecturalstyletype USING (arcitecturalstyletypeid)
LEFT JOIN buildingclasstype USING (buildingclasstypeid)
LEFT JOIN heatingorsystemtype USING (heatingorsystemtypeid)
LEFT JOIN propertylandusetype USING (propertylandusetypeid)
LEFT JOIN storytype USING (storytypeid)
LEFT JOIN typeconstructiontype USING (typeconstructiontypeid)
JOIN predictions_2017 ON properties_2017.parcelid = predictions_2017.parcelid 
AND predictions_2017.transactiondate LIKE '2017%%'




'''