{{ config(materialized='table') }}

WITH employee_base AS (
    SELECT * FROM {{ ref('model1') }}
)

SELECT
    Department,
    COUNT(*) AS employee_count,
    MAX(Last_modified_timestamp) AS latest_update
FROM employee_base
GROUP BY Department