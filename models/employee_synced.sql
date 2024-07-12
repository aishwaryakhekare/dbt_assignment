{{ config(materialized='table') }}

with snapshot_data as (
    select *,
           row_number() over (partition by EMPLOYEE_ID order by DBT_VALID_TO desc) as rn
    from {{ ref('employee_snapshot') }}
    where DBT_VALID_TO is null  -- This selects only the current valid records
),

source_data as (
    select *
    from {{ source('raw', 'TEST_DATA') }}
),

updated_records as (
    select s.*
    from source_data s
    left join snapshot_data sd on s.EMPLOYEE_ID = sd.EMPLOYEE_ID
    where sd.EMPLOYEE_ID is null  -- New records
       or s.Last_modified_timestamp > sd.Last_modified_timestamp  -- Updated records
),

final as (
    select 
        coalesce(ur.EMPLOYEE_ID, sd.EMPLOYEE_ID) as EMPLOYEE_ID,
        coalesce(ur.EMPLOYEE_FIRST_NAME, sd.EMPLOYEE_FIRST_NAME) as EMPLOYEE_FIRST_NAME,
        coalesce(ur.EMPLOYEE_LAST_NAME, sd.EMPLOYEE_LAST_NAME) as EMPLOYEE_LAST_NAME,
        coalesce(ur.Designation, sd.Designation) as Designation,
        coalesce(ur.Department, sd.Department) as Department,
        coalesce(ur.Employee_email, sd.Employee_email) as Employee_email,
        coalesce(ur.EMPLOYEE_PHONE_NUMBER, sd.EMPLOYEE_PHONE_NUMBER) as EMPLOYEE_PHONE_NUMBER,
        coalesce(ur.EMPLOYEE_ADDRESS, sd.EMPLOYEE_ADDRESS) as EMPLOYEE_ADDRESS,
        coalesce(ur.Last_modified_timestamp, sd.Last_modified_timestamp) as Last_modified_timestamp,
        case 
            when ur.EMPLOYEE_ID is not null then 'Updated/New'
            else 'Unchanged'
        end as Change_Status
    from snapshot_data sd
    full outer join updated_records ur on sd.EMPLOYEE_ID = ur.EMPLOYEE_ID
    where sd.rn = 1 or sd.rn is null  -- This ensures we only get the latest snapshot record
)

select * from final