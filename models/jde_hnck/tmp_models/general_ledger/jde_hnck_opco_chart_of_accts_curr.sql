with jde_hnck_opco_chart_of_accts as(
    select *,
    rank() over(partition by opco_chart_of_accts_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('jde_opco_chart_of_accts') }}
),
final as(
    select 
    *
    from jde_hnck_opco_chart_of_accts
    where rnk = 1 and delete_dtm is null
)

select * from final

