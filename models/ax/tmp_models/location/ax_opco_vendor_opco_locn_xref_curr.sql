with ax_opco_vendor_opco_locn_xref as(
    select *,
    rank() over(partition by opco_vendor_sk, opco_locn_sk, opco_vendor_opco_locn_asscn_type_txt order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_opco_vendor_opco_locn_xref') }}
),
final as(
    select 
    *
    from ax_opco_vendor_opco_locn_xref
    where rnk = 1 and delete_dtm is null
)

select * from final