with ax_opco_invtry_trans as(
    select *,
    rank() over(partition by opco_invtry_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_opco_invtry_trans') }}
),
final as(
    select 
    *
    from ax_opco_invtry_trans
    where rnk = 1 and delete_dtm is null
)

select * from final