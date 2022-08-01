with v_ax_opco_vendor_trans  as(
    select *,
    rank() over(partition by opco_vendor_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_trans
),
final as(
    select 
    *
    from v_ax_opco_vendor_trans 
    where rnk = 1 and delete_dtm is null
)

select * from final