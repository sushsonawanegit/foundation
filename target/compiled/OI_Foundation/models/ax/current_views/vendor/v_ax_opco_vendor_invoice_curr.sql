with v_ax_opco_vendor_invoice as(
    select *,
    rank() over(partition by opco_vendor_invoice_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_invoice
),
final as(
    select 
    *
    from v_ax_opco_vendor_invoice
    where rnk = 1 and delete_dtm is null
)

select * from final