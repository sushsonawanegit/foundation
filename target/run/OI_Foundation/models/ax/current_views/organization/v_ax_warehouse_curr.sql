
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_ax_warehouse_curr 
  
   as (
    with v_ax_warehouse as(
    select *,
    rank() over(partition by warehouse_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_warehouse
),
final as(
    select 
    *
    from v_ax_warehouse
    where rnk = 1 and delete_dtm is null
)

select * from final
  );
