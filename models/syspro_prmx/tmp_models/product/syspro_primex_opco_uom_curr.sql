with syspro_primex_opco_uom as(
    select *,
    rank() over(partition by opco_uom_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('syspro_primex_opco_uom') }}
),
final as(
    select 
    *
    from syspro_primex_opco_uom
    where rnk = 1 and delete_dtm is null
)

select * from final