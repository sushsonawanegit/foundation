with ax_opco_vendor as(
    select *,
    rank() over(partition by opco_vendor_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_opco_vendor') }}
),
final as(
    select 
    *
    from ax_opco_vendor
    where rnk = 1 and delete_dtm is null
)

select * from final
