
  
    

  create  table "postgres"."public_pipedrive_analytics"."stg_pipedrive__stages__dbt_tmp"
  
  
    as
  
  (
    -- Clean and standardize pipeline stages
with source as (
    select * from "postgres"."public"."stages"
),

cleaned as (
    select
        stage_id,
        stage_name,
        -- Map stage names to funnel steps
        case
            when lower(stage_name) like '%lead%gen%' then 'Step 1: Lead Generation'
            when lower(stage_name) like '%qualif%' then 'Step 2: Qualified Lead'
            when lower(stage_name) like '%needs%assess%' then 'Step 3: Needs Assessment'
            when lower(stage_name) like '%proposal%' or lower(stage_name) like '%quote%' then 'Step 4: Proposal/Quote Preparation'
            when lower(stage_name) like '%negotiat%' then 'Step 5: Negotiation'
            when lower(stage_name) like '%clos%' then 'Step 6: Closing'
            when lower(stage_name) like '%implement%' or lower(stage_name) like '%onboard%' then 'Step 7: Implementation/Onboarding'
            when lower(stage_name) like '%follow%' or lower(stage_name) like '%success%' then 'Step 8: Follow-up/Customer Success'
            when lower(stage_name) like '%renewal%' or lower(stage_name) like '%expansion%' then 'Step 9: Renewal/Expansion'
            else stage_name
        end as funnel_step
    from source
)

select * from cleaned
  );
  