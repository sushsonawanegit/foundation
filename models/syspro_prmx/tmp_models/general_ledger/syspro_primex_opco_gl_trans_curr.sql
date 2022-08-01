with syspro_primex_opco_gl_trans as(
    select *,
    rank() over(partition by opco_gl_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('syspro_primex_opco_gl_trans') }}
),
final as(
    select 
    *
    from syspro_primex_opco_gl_trans
    where rnk = 1 and delete_dtm is null
)

select * from final