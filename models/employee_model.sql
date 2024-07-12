{{
    config(
        materialized='table',
        pre_hook=[
            "create table if not exists {{ this.schema }}.earliest_date_log (model_name string, earliest_date timestamp)",
            "insert into {{ this.schema }}.earliest_date_log (model_name, earliest_date) select '{{ this.name }}', {{ get_earliest_date() }}"
        ]
    )
}}

select *
from {{ source('raw', 'TEST_DATA') }}
where Last_modified_timestamp >= {{ get_earliest_date() }}