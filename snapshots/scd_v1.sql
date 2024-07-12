{% snapshot scd2_v2 %}

{{
    config(
        target_schema ='snapshot',
        unique_key ='employee_id',
        strategy='check',
        check_cols='all'
    )
}}
select * from {{source('raw','employee_details')}}

{% endsnapshot %}