{{ config(
    materialized='table',
    pre_hook="select count(*) as row_count from {{this}}"
    
) }}
with cte as (
    select
        EMPLOYEE_ID,
        concat(EMPLOYEE_FIRST_NAME,' ', EMPLOYEE_LAST_NAME) as full_name,
        DESIGNATION,
        DEPARTMENT,
        EMPLOYEE_EMAIL,
        EMPLOYEE_PHONE_NUMBER,
        EMPLOYEE_ADDRESS
    from {{ ref('model1') }}
    where EMPLOYEE_FIRST_NAME is not null
      and EMPLOYEE_LAST_NAME is not null
)
select * from cte