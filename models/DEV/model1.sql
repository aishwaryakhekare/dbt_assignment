{{ config(materialized='table') }}

SELECT
    EMPLOYEE_ID,
    EMPLOYEE_FIRST_NAME,
    EMPLOYEE_LAST_NAME,
    Department,
    Last_modified_timestamp
FROM {{ source('raw', 'TEST_DATA') }}