
  
    

  create  table "postgres"."public_pipedrive_analytics"."rep_sales_funnel_monthly__dbt_tmp"
  
  
    as
  
  (
    -- Monthly sales funnel report with all KPIs
with deals_with_stages as (
    select * from "postgres"."public_pipedrive_analytics"."int_deals_with_stages"
),

sales_calls as (
    select * from "postgres"."public_pipedrive_analytics"."int_sales_calls"
),

-- Main funnel metrics
funnel_metrics as (
    -- Step 1: Lead Generation (all deals created)
    select
        created_month as month,
        'Deals Created' as kpi_name,
        'Step 1: Lead Generation' as funnel_step,
        count(distinct deal_id) as deals_count
    from deals_with_stages
    where created_month is not null
    group by 1, 2, 3

    union all

    -- Step 2: Qualified Lead (deals in qualified stage)
    select
        created_month as month,
        'Qualified Leads' as kpi_name,
        'Step 2: Qualified Lead' as funnel_step,
        count(distinct deal_id) as deals_count
    from deals_with_stages
    where created_month is not null
        and funnel_step = 'Step 2: Qualified Lead'
    group by 1, 2, 3

    union all

    -- Step 2.1: Sales Call 1 (deals with first call)
    select
        activity_month as month,
        'First Sales Call Completed' as kpi_name,
        'Step 2.1: Sales Call 1' as funnel_step,
        count(distinct deal_id) as deals_count
    from sales_calls
    where activity_month is not null
        and call_number = 1
    group by 1, 2, 3

    union all

    -- Step 3: Needs Assessment
    select
        created_month as month,
        'Needs Assessment' as kpi_name,
        'Step 3: Needs Assessment' as funnel_step,
        count(distinct deal_id) as deals_count
    from deals_with_stages
    where created_month is not null
        and funnel_step = 'Step 3: Needs Assessment'
    group by 1, 2, 3

    union all

    -- Step 3.1: Sales Call 2 (deals with second call)
    select
        activity_month as month,
        'Second Sales Call Completed' as kpi_name,
        'Step 3.1: Sales Call 2' as funnel_step,
        count(distinct deal_id) as deals_count
    from sales_calls
    where activity_month is not null
        and call_number = 2
    group by 1, 2, 3

    union all

    -- Step 4: Proposal/Quote Preparation
    select
        created_month as month,
        'Proposal Sent' as kpi_name,
        'Step 4: Proposal/Quote Preparation' as funnel_step,
        count(distinct deal_id) as deals_count
    from deals_with_stages
    where created_month is not null
        and funnel_step = 'Step 4: Proposal/Quote Preparation'
    group by 1, 2, 3

    union all

    -- Step 5: Negotiation
    select
        created_month as month,
        'In Negotiation' as kpi_name,
        'Step 5: Negotiation' as funnel_step,
        count(distinct deal_id) as deals_count
    from deals_with_stages
    where created_month is not null
        and funnel_step = 'Step 5: Negotiation'
    group by 1, 2, 3

    union all

    -- Step 6: Closing (won deals)
    select
        created_month as month,
        'Deals Won' as kpi_name,
        'Step 6: Closing' as funnel_step,
        count(distinct deal_id) as deals_count
    from deals_with_stages
    where created_month is not null
        and deal_status = 'won'
    group by 1, 2, 3

    union all

    -- Step 7: Implementation/Onboarding
    select
        created_month as month,
        'Onboarding Started' as kpi_name,
        'Step 7: Implementation/Onboarding' as funnel_step,
        count(distinct deal_id) as deals_count
    from deals_with_stages
    where created_month is not null
        and funnel_step = 'Step 7: Implementation/Onboarding'
    group by 1, 2, 3

    union all

    -- Step 8: Follow-up/Customer Success
    select
        created_month as month,
        'Customer Success Active' as kpi_name,
        'Step 8: Follow-up/Customer Success' as funnel_step,
        count(distinct deal_id) as deals_count
    from deals_with_stages
    where created_month is not null
        and funnel_step = 'Step 8: Follow-up/Customer Success'
    group by 1, 2, 3

    union all

    -- Step 9: Renewal/Expansion
    select
        created_month as month,
        'Renewal Opportunities' as kpi_name,
        'Step 9: Renewal/Expansion' as funnel_step,
        count(distinct deal_id) as deals_count
    from deals_with_stages
    where created_month is not null
        and funnel_step = 'Step 9: Renewal/Expansion'
    group by 1, 2, 3
)

select
    to_char(month, 'YYYY-MM') as month,
    kpi_name,
    funnel_step,
    deals_count
from funnel_metrics
where month is not null
order by month, funnel_step
  );
  