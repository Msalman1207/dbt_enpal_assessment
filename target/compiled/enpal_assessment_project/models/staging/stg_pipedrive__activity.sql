-- Clean activities data
with source as (
    select * from "postgres"."public"."activity"
),

cleaned as (
    select
        activity_id,
        deal_id,
        type as activity_type,
        assigned_to_user,
        done as is_completed,
        due_to::timestamp as activity_date,
        date_trunc('month', due_to::timestamp) as activity_month,
        -- Classify activity types
        case
            when lower(type) like '%call%' or lower(type) = 'sc_2' then 'Sales Call'
            when lower(type) like '%meeting%' then 'Meeting'
            when lower(type) like '%email%' then 'Email'
            when lower(type) like '%follow%' then 'Follow-up'
            else type
        end as activity_category
    from source
    where deal_id is not null
)

select * from cleaned