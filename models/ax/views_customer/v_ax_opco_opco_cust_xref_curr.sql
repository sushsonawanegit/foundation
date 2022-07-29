with v_ax_opco_opco_cust_xref_curr as(
    select *,
    rank() over(partition by opco_sk, opco_cust_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_opco_opco_cust_xref') }}
),
final as(
    select 
    *
    from v_ax_opco_opco_cust_xref_curr
    where rnk = 1 and delete_dtm is null
)

select * from final
