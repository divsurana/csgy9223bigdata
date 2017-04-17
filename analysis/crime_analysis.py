import pandas as pd
import numpy as np
import matplotlib.pylab as plt
from datetime import datetime
from matplotlib.pylab import rcParams
rcParams['figure.figsize'] = 15, 6


#chunksize = 5
data = pd.read_csv('/home/karan/crime.csv', dtype={"HADEVELOPT": str ,"PREM_TYP_DESC":str,"PARKS_NM":str})
#DtypeWarning: Columns (17) have mixed types. Specify dtype option on import or set low_memory=False.
#full_data = pd.concat(data, ignore_index=True)

print data.head()
print '\n Data Types:'
print data.dtypes

dfreport=pd.DataFrame(data , columns=['RPT_DT'])
print dfreport.head()
dfreport['RPT_DT'] = pd.to_datetime(dfreport['RPT_DT'])

dfreport['freq']=dfreport.groupby('RPT_DT')['RPT_DT'].transform('count')

df1=dfreport.drop_duplicates(['RPT_DT'])
print df1.head()
df1.plot(['RPT_DT'],['freq'])
plt.show()
