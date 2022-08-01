
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_ax_opco_item_psi_dtl_curr 
  
   as (
    with v_ax_opco_item_psi_dtl as(
    select *,
    rank() over(partition by opco_item_psi_dtl_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_psi_dtl
),
final as(
    select 
    *
    from v_ax_opco_item_psi_dtl
    where rnk = 1 and delete_dtm is null
)

select * from final
  );
