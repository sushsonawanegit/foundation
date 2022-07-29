with v_ax_opco_cust_settled_trans_curr as(
    select *,
    rank() over(partition by opco_cust_settled_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_opco_cust_settled_trans') }}
),
final as(
    select 
    *
    from v_ax_opco_cust_settled_trans_curr
    where rnk = 1 and delete_dtm is null
)

select * from final
