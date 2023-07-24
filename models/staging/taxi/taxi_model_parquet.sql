{% set file_format='parquet' %}
{% set s3location='s3://sales-sandbox-databricks-unity-catalog/s3data/taxi-data/parquet' %}


{{
    config(
        materialized='copy_into',
        file_format = file_format, 
        header = true, 
        inferSchema = true, 
        mergeSchema = true,
        s3location = s3location
    )
}}

select * FROM {{file_format}}.`{{s3location}}`