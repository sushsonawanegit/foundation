with jde_hnck_opco_currency as(
    select *,
    rank() over(partition by opco_currency_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('jde_opco_currency') }}
),
final as(
    select 
    *
    from jde_hnck_opco_currency
    where rnk = 1 and delete_dtm is null
)

select * from final