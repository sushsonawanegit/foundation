with syspro_primex_opco_fscl_yr_gl_opening_bal as(
    select *,
    rank() over(partition by opco_fscl_yr_gl_opening_bal_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('syspro_primex_opco_fscl_yr_gl_opening_bal') }}
),
final as(
    select 
    *
    from syspro_primex_opco_fscl_yr_gl_opening_bal
    where rnk = 1 and delete_dtm is null
)

select * from final

