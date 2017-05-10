import plotly.tools as tls
tls.set_credentials_file(username='divsurana', api_key='kDxm1TMATbyqFU8IqQa2')
# Note that the key may be invalid when the program is re-run

import pandas as pd
import plotly.plotly as py
import plotly.graph_objs as go  # interactive graphing
from datetime import datetime
from scipy import stats
import holidays

us_holidays = holidays.UnitedStates()

df = pd.read_csv("../NYPD_Complaint_Data_Historic.csv", usecols=[0, 1, 2, 3, 4, 5])
refined_df = pd.DataFrame(df['CMPLNT_NUM'].groupby(df['CMPLNT_FR_DT']).count().reset_index())

# transfer to DateTime type and add weekday/weekend, holiday/non holiday information

us_holidays = holidays.UnitedStates()

for i in range(len(refined_df)):
	try:
		refined_df.ix[i, 'CMPLNT_FR_DT'] = datetime.strptime(refined_df.ix[i, 'CMPLNT_FR_DT'], "%m/%d/%Y")
		refined_df.ix[i, 'YEAR'] = refined_df.ix[i, 'CMPLNT_FR_DT'].year
		refined_df.ix[i, 'MONTH'] = refined_df.ix[i, 'CMPLNT_FR_DT'].month
		refined_df.ix[i, 'DAY'] = refined_df.ix[i, 'CMPLNT_FR_DT'].day
		refined_df.ix[i, 'If_Public_Holiday'] = refined_df.ix[i, 'CMPLNT_FR_DT'] in us_holidays
		refined_df.ix[i, 'WEEKDAY'] = refined_df.ix[i, 'CMPLNT_FR_DT'].weekday()
	except:
		print(refined_df.ix[i, 'CMPLNT_FR_DT'])
	
# only analyzing from 2005 Jan to 2015 Dec
refined_df = refined_df[(refined_df['YEAR'] < 2016) & (refined_df['YEAR'] > 2005)]
	
# visualize the number of crimes per year
df_year = pd.DataFrame(refined_df['CMPLNT_NUM'].groupby(refined_df['YEAR']).sum().reset_index())
year = go.Scatter(
	x=df_year['YEAR'],
	y=df_year['CMPLNT_NUM'],
	name='No of crimes per year'
)

data = [year]
layout = go.Layout(
	title='No of crimes per year')
fig = go.Figure(data=data, layout=layout)
py.image.save_as(fig, filename='No_of_crimes_per_year.png')

# visualize the number of crimes per month
df_month = pd.DataFrame(refined_df['CMPLNT_NUM'].groupby(refined_df['MONTH']).sum().reset_index())
month = go.Scatter(
	x=df_month['MONTH'],
	y=df_month['CMPLNT_NUM'],
	name='No of crimes per month'
)

data = [month]
layout = go.Layout(
	title='No of crimes per month')
fig = go.Figure(data=data, layout=layout)
py.image.save_as(fig, filename='No_of_crimes_per_month.png')

# visualize the number of crimes per day in a month
df_day = pd.DataFrame(refined_df['CMPLNT_NUM'].groupby(refined_df['DAY']).sum().reset_index())
day = go.Scatter(
	x=df_day['DAY'],
	y=df_day['CMPLNT_NUM'],
	name='No of crimes per day'
)

data = [day]
layout = go.Layout(
	title='No of crimes per day')
fig = go.Figure(data=data, layout=layout)
py.image.save_as(fig, filename='No_of_crimes_per_day.png')

# visualize the number of crimes per weekday
df_weekday = pd.DataFrame(refined_df['CMPLNT_NUM'].groupby(refined_df['WEEKDAY']).sum().reset_index())
df_weekday['WEEKDAY'] = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']
weekday = go.Bar(
	x=df_weekday['WEEKDAY'],
	y=df_weekday['CMPLNT_NUM'],
	name='No of crimes per weekday',
	marker=dict(
		color=['rgba(192,192,192,1)', 'rgba(192,192,192,1)', 'rgba(192,192,192,1)',
		       'rgba(192,192,192,1)', 'rgba(192,192,192,1)',
		       'rgba(255,0,0,1)', 'rgba(255,0,0,1)']),
)

data = [weekday]
layout = go.Layout(
	title='No of crimes per weekday')
fig = go.Figure(data=data, layout=layout)
py.image.save_as(fig, filename='No_of_crimes_per_weekday.png')

# calculate correlation coefficients
weekday = refined_df['CMPLNT_NUM'].groupby([refined_df['WEEKDAY'], refined_df['YEAR']]).sum().unstack().reset_index()
weekday['If_Weekend'] = weekday['WEEKDAY'].map(lambda x: x == 5 or x == 6)
weekday = weekday.groupby(weekday['If_Weekend']).mean().reset_index()
week = [weekday.ix[0, i] for i in range(3, 12)]
weekend = [weekday.ix[1, i] for i in range(3, 12)]

stat, pvalue = stats.ttest_ind(week, weekend)
print("The p-value for this test is", pvalue)

# visualize the number of crimes per day in 2015 including holiday/non-holiday
new_df_2015 = refined_df[refined_df['YEAR'] == 2015].reset_index()

color_list = []
for i in range(len(new_df_2015)):
	if new_df_2015.ix[i, 'If_Public_Holiday'] is True:
		color_list.append('rgba(255,0,0,1)')
	else:
		color_list.append('rgba(192,192,192,1)')

plot_2015 = go.Bar(
	x=new_df_2015['CMPLNT_FR_DT'],
	y=new_df_2015['CMPLNT_NUM'],
	name='No of crimes per day for the year 2015 (Holidays included)',
	marker=dict(
		color=color_list),
)
data = [plot_2015]
layout = go.Layout(
	title='No of crimes per day for the year 2015 (Holidays included)')
fig = go.Figure(data=data, layout=layout)
py.iplot(fig)
py.image.save_as(fig, filename='No_of_crimes_per_day_2015.png')

# calculate correlation coefficients
holiday = refined_df['CMPLNT_NUM'].groupby([refined_df['If_Public_Holiday'], refined_df['YEAR']]).mean().unstack().reset_index()
holiday_mean = [holiday.ix[0, i] for i in range(2, 11)]
non_holiday_mean = [holiday.ix[1, i] for i in range(2, 11)]
stat, pvalue = stats.ttest_ind(holiday_mean, non_holiday_mean)
print("The p-value for this test is", pvalue)
