{% macro get_earliest_date() %}
    (select min(Last_modified_timestamp)::timestamp from {{ source('raw', 'TEST_DATA') }})
{% endmacro %}