{% snapshot employee_snapshot %}

{{
    config(
      target_schema='snapshot',
      strategy='timestamp',
      unique_key='EMPLOYEE_ID',
      updated_at='Last_modified_timestamp',
    )
}}

select * from {{ source('raw', 'TEST_DATA') }}

{% endsnapshot %}