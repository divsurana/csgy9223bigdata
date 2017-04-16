import pandas as pd
import numpy as np
import matplotlib.pylab as plt

from matplotlib.pylab import rcParams
rcParams['figure.figsize'] = 15, 6


#chunksize = 5
data = pd.read_csv('/home/karan/crime.csv', dtype={"HADEVELOPT": str ,"PREM_TYP_DESC":str,"PARKS_NM":str})

#DtypeWarning: Columns (17) have mixed types. Specify dtype option on import or set low_memory=False.
#full_data = pd.concat(data, ignore_index=True)

print data.head()
print '\n Data Types:'
print data.dtypes
