# BIG DATA ANALYTICS Spring 2017 Project

# Analysing the NYPD Crime data 

[Available here] (https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i)

**The analysis so far consists of three parts**
1) Summary (here we have aggregated the column data to get a basic idea of the dataset)

2)Column_details ( consists of the column details scripts providing the following categories for each value - 

base type , semantic type , INVALID/NULL

3)Analysis ( provides us with the python script which uses python pandas and matplotlib to analyse the dataset and plot various graphs)



**Clone the repository** 
 `git clone https://github.com/divsurana/csgy9223bigdata.git`

**Enter the project repository**
`cd csgy9223bigdata/`

**Pull the dataset using wget** 
`wget -O crime.csv https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD`

This will retreive the csv file and rename it to crime.csv


**STEPS TO RUN THE SUMMARY CODE -**

1)Now run the column aggregation scripts which are in the summary folder by 

`sh run_summary.sh crime.csv`

2)Then to retreive outputs of those scripts in the results folder run

`sh get_out_summary.sh`

3)Finally to remove all the outputs from your hadoop filesystem run 

`sh del_summary.sh`

**STEPS TO RUN THE COLUMN DETAILS CODE-** 
1)Now run the column details scripts which are in the column_details folder by 

`sh run_column_details.sh crime.csv`

2)Then to retreive outputs of those scripts in the results folder run

`sh get_out_columns.sh`

3)Finally to remove all the outputss from your hadoop filesystem run

`sh del_column_details.sh`


**STEPS TO RUN THE ANALYSIS CODE -**
(requires python pandas,numpy and matplotlib libraries)

To run the analysis code run 

`sh run_analysis.sh crime.csv`

