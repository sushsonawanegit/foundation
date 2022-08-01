with ax_opco_assctn as(
    select *,
    rank() over(partition by opco_assctn_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_opco_assctn') }}
),
final as(
    select 
    *
    from ax_opco_assctn
    where rnk = 1 and delete_dtm is null
)

select * from final