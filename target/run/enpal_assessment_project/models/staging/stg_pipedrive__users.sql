
  
    

  create  table "postgres"."public_pipedrive_analytics"."stg_pipedrive__users__dbt_tmp"
  
  
    as
  
  (
    -- Clean users reference data
with source as (
    select * from "postgres"."public"."users"
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
  );
  