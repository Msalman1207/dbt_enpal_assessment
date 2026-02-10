-- Clean users reference data
with source as (
    select * from {{ source('pipedrive_raw', 'users') }}
),

cleaned as (
    select
        id as user_id,
        name as user_name,
        email as user_email,
        modified::timestamp as last_modified
    from source
)

select * from cleaned
