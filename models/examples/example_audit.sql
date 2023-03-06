

{{
    config(
        pre_hook="{{ audit_model_start() }}",
        post_hook="{{ audit_model_end() }}"

    )
}}

select 1 as id