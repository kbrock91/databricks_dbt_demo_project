{% macro audit_model_start() %}

    insert into dbt_kbrock.dummy_audit_table (
PROCESS_UNIT	,
PROCESS_STATUS_ID	,
SOURCE_ROWS	,
TARGET_ROWS	,
LOAD_DURATION,
CUSTOM_LOGS_JSON	,
RUN_ID,
row_created_date, 
row_updated_date
    )

    values (

        '{{this.name}}', 
        -1, 
        null, 
        null, 
        null,
        null,
        '{{ invocation_id }}',
        current_timestamp, 
        null
    )



{% endmacro %}

{% macro audit_model_end() %}
{% if execute %}
{% set target_row_count_query %}
select count(*)
from {{this.schema}}.{{this.name}}
{% endset %}
{% set results = run_query(target_row_count_query) %}

{# Return the first column #}
{% set target_row_count = results.columns[0].values()[0] %}
{% endif %}

    update dbt_kbrock.dummy_audit_table 

    set 
        PROCESS_STATUS_ID = 0,
        SOURCE_ROWS	= 0 ,
        TARGET_ROWS	= {{target_row_count}} ,
        LOAD_DURATION = datediff(current_timestamp, row_created_date), 
        CUSTOM_LOGS_JSON = 0 , 
        row_updated_date = current_timestamp 
        

    where 
        RUN_ID = '{{ invocation_id }}'
        and 
        process_unit = '{{this.name}}'


{% endmacro %}