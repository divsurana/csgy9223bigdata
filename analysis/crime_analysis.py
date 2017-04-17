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
print data.count()

#plot count of all columns
data.count().plot.bar()

#plot date vs no of crime occurences on that date
dfreport=pd.DataFrame(data , columns=['RPT_DT'])
dfreport['RPT_DT'] = pd.to_datetime(dfreport['RPT_DT'])
dfreport['freq']=dfreport.groupby('RPT_DT')['RPT_DT'].transform('count')

df1=dfreport.drop_duplicates(['RPT_DT'])
df1.plot(['RPT_DT'],['freq'])

#plot time vs no of complaints made at that time
df1report=pd.DataFrame(data , columns=['CMPLNT_FR_TM'])
df1report['CMPLNT_FR_TM'] = pd.to_datetime(df1report['CMPLNT_FR_TM'])
df1report['freq']=df1report.groupby('CMPLNT_FR_TM')['CMPLNT_FR_TM'].transform('count')

df2=df1report.drop_duplicates(['CMPLNT_FR_TM'])
df2.plot(['CMPLNT_FR_TM'],['freq'])

#plot boroughs vs the no of crime occurances in each borough
boro = data.groupby('BORO_NM')
boro.size().plot.bar()

plt.show()




