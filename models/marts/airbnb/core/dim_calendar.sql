{{ config(materialized='table', zorder="calendar_id" ) }}

with source as (

    select * from {{ ref('stg_airbnb_calendar') }}

)

select * from source