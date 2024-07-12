{{
    config(
        materialized='ephemeral'
    )
}}

with cte as(
    select * from {{source('raw','employee_details')}}
)

select * from cte