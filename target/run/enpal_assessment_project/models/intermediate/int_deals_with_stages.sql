
  
    

  create  table "postgres"."public_pipedrive_analytics"."int_deals_with_stages__dbt_tmp"
  
  
    as
  
  (
    -- Join deals with their stage information
with deals as (
    select * from "postgres"."public_pipedrive_analytics"."stg_pipedrive__deals"
),

stages as (
    select * from "postgres"."public_pipedrive_analytics"."stg_pipedrive__stages"
),

users as (
    select * from "postgres"."public_pipedrive_analytics"."stg_pipedrive__users"
),

joined as (
    select
        d.deal_id,
        d.created_at,
        d.created_month,
        d.owner_id,
        u.user_name,
        u.user_email,
        d.stage_id,
        s.stage_id as stage_id_ref,
        s.stage_name,
        s.funnel_step,
        d.deal_status,
        d.status as raw_status,
        d.lost_reason
    from deals d
    left join stages s on d.stage_id = s.stage_id
    left join users u on d.owner_id = u.user_id
)

select * from joined
  );
  