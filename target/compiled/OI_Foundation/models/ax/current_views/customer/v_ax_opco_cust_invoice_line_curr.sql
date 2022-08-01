with v_ax_opco_cust_invoice_line as(
    select *,
    rank() over(partition by opco_cust_invoice_line_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_invoice_line
),
final as(
    select 
    *
    from v_ax_opco_cust_invoice_line
    where rnk = 1 and delete_dtm is null
)

select * from final