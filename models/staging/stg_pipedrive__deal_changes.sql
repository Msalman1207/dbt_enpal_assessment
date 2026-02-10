-- Clean and standardize deal changes data
with source as (
    select * from {{ source('pipedrive_raw', 'deal_changes') }}
),

cleaned as (
    select
        deal_id,
        change_time::timestamp as change_time,
        changed_field_key,
        new_value,
        row_number() over (
            partition by deal_id, changed_field_key 
            order by change_time
        ) as change_sequence
    from source
    where deal_id is not null
)

select * from cleaned
