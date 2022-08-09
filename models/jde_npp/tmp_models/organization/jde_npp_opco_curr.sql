with jde_npp_opco_curr as(
    select *,
    rank() over(partition by opco_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('jde_npp_opco')}}
),
final as(
    select 
    *
    from jde_npp_opco_curr
    where rnk = 1 and delete_dtm is null
)

select * from final