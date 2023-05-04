{{
    config(
        materialized='copy_into',
        file_format = 'CSV', 
        header = true, 
        inferSchema = true, 
        mergeSchema = true
    )
}}

select * FROM 's3://sales-sandbox-databricks-unity-catalog/s3data/taxi-data'