
  
    

  create  table "postgres"."public_pipedrive_analytics"."stg_pipedrive__deals__dbt_tmp"
  
  
    as
  
  (
    -- Build deals from deal_changes history 
-- Using the latest values for each deal field
with deal_changes as (
    select * from "postgres"."public_pipedrive_analytics"."stg_pipedrive__deal_changes"
),

-- Get the latest value for each field per deal
deal_pivoted as (
    select
        deal_id,
        max(case when changed_field_key = 'add_time' then new_value end) as created_at_str,
        max(case when changed_field_key = 'user_id' then new_value::int end) as owner_id,
        max(case when changed_field_key = 'stage_id' then new_value::int end) as stage_id,
        max(case when changed_field_key = 'status' then new_value end) as status,
        max(case when changed_field_key = 'lost_reason' then new_value end) as lost_reason
    from deal_changes
    group by deal_id
),

-- Format and enrich
cleaned as (
    select
        deal_id,
        created_at_str::timestamp as created_at,
        date_trunc('month', created_at_str::timestamp) as created_month,
        owner_id,
        stage_id,
        status,
        lost_reason,
        -- Determine deal outcome
        case 
            when status = 'won' then 'won'
            when status = 'lost' then 'lost'
            else 'open'
        end as deal_status
    from deal_pivoted
    where deal_id is not null
)

select * from cleaned
  );
  