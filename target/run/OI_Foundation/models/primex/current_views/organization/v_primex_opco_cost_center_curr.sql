
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_primex_opco_cost_center_curr 
  
   as (
    with v_primex_opco_cost_center as(
    select *,
    rank() over(partition by opco_cost_center_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_cost_center
),
final as(
    select 
    *
    from v_primex_opco_cost_center
    where rnk = 1 and delete_dtm is null
)

select * from final
  );
