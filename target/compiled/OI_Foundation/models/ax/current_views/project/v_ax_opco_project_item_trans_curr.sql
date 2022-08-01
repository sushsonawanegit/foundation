with v_ax_opco_project_item_trans as(
    select *,
    rank() over(partition by opco_project_item_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_item_trans
),
final as(
    select 
    *
    from v_ax_opco_project_item_trans 
    where rnk = 1 and delete_dtm is null
)

select * from final