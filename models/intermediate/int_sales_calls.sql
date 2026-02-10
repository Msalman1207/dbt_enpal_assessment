-- Identify and rank sales calls for each deal
with activities as (
    select * from {{ ref('stg_pipedrive__activity') }}
),

sales_calls as (
    select
        deal_id,
        activity_id,
        activity_date,
        activity_type,
        activity_month,
        is_completed,
        row_number() over (
            partition by deal_id 
            order by activity_date
        ) as call_number
    from activities
    where activity_category = 'Sales Call'
        and is_completed = true
)

select * from sales_calls
