
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_ax_opco_item_model_grp_curr 
  
   as (
    with v_ax_opco_item_model_grp as(
    select *,
    rank() over(partition by opco_item_model_grp_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_model_grp
),
final as(
    select 
    *
    from v_ax_opco_item_model_grp
    where rnk = 1 and delete_dtm is null
)

select * from final
  );
