{% materialization copy_into, adapter='databricks' %}

    {%- set file_format = config.get('file_format') -%}
    {%- set header = config.get('header') -%}
    {%- set inferSchema = config.get('inferSchema') -%}
    {%- set mergeSchema = config.get('mergeSchema') -%}
    {%- set s3location = config.get('s3location') -%}

    {%- set identifier = model['alias'] -%}

    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database, type='table') -%}

    {{ run_hooks(pre_hooks) }}


    {% if old_relation is none or not old_relation.is_table or should_full_refresh() %}
        {{ log("Replacing existing relation " ~ old_relation) }}
        {%- call statement('main') -%}
            CREATE TABLE IF NOT EXISTS {{target_relation}} 
        {%- endcall -%}
    {% endif %}


        {%- call statement('main') -%}
            COPY INTO {{target_relation}} 
            FROM '{{s3location}}'
            FILEFORMAT = {{file_format}}
            FORMAT_OPTIONS (
                'header' = '{{header}}',
                'inferSchema' = '{{inferSchema}}'
                )
            COPY_OPTIONS (
                'mergeSchema' = '{{mergeSchema}}'
                );

        {%- endcall -%}



    {{ run_hooks(post_hooks) }}

    {% do persist_docs(target_relation, model) %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}