with v_ax_opco_item_subtype as(
    select *,
    rank() over(partition by opco_item_subtype_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_subtype
),
final as(
    select 
    *
    from v_ax_opco_item_subtype
    where rnk = 1 and delete_dtm is null
)

select * from final