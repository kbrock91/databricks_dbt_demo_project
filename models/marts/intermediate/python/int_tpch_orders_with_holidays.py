import holidays
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType


def is_holiday(date_col):
    us_holidays = holidays.US()
    is_holiday = (date_col in us_holidays)
    return is_holiday

holidayUDF = udf(lambda x:is_holiday(x),BooleanType())  

def model(dbt, session):
    dbt.config(
        materialized = "table",
        packages = ["holidays"], #import pypi holidays package
        submission_method="all_purpose_cluster",
        cluster_id="1215-215802-worukqlx", 
        create_notebook = True
    )

    orders_df = dbt.ref("stg_tpch_orders")

    # apply our function
    df = orders_df.withColumn("is_holiday", holidayUDF(col("order_date")))           

    # return final dataset (PySpark DataFrame)
    return df