with ax_opco_project_invoice_project_xref  as(
    select *,
    rank() over(partition by opco_project_sk, invoice_project_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_opco_project_invoice_project_xref') }}
),
final as(
    select 
    *
    from ax_opco_project_invoice_project_xref
    where rnk = 1 and delete_dtm is null
)

select * from final