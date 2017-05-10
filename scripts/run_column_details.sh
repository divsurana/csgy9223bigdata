spark-submit ./column_details/add_pct_code_details.py $1
spark-submit ./column_details/complaint_number.py $1
spark-submit ./column_details/crime_completed_desc.py $1
spark-submit ./column_details/law_cat_cd.py $1
spark-submit ./column_details/offense_desc.py $1
spark-submit ./column_details/pd_desc.py  $1
spark-submit ./column_details/boros.py $1
spark-submit ./column_details/complaint_time_details.py $1
spark-submit ./column_details/key_code_details.py $1
spark-submit ./column_details/location_details.py $1
spark-submit ./column_details/pd_code_details.py $1
spark-submit ./column_details/report_date_details.py $1

