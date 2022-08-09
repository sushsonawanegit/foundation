with ax_opco_item_class as(
    select *,
    rank() over(partition by opco_item_class_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_class
),
final as(
    select 
    *
    from ax_opco_item_class
    where rnk = 1 and delete_dtm is null
)

select * from final