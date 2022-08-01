with ax_opco_vendor_invoice as(
    select *,
    rank() over(partition by opco_vendor_invoice_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_opco_vendor_invoice') }}
),
final as(
    select 
    *
    from ax_opco_vendor_invoice
    where rnk = 1 and delete_dtm is null
)

select * from final