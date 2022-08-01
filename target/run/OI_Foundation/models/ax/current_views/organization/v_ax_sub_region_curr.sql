
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_ax_sub_region_curr 
  
   as (
    with v_ax_sub_region as(
    select *,
    rank() over(partition by sub_region_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_sub_region
),
final as(
    select 
    *
    from v_ax_sub_region
    where rnk = 1 and delete_dtm is null
)

select * from final
  );
