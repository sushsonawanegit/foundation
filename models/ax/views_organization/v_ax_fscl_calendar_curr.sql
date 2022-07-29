with v_ax_fscl_clndr_curr as(
    select *,
    rank() over(partition by fscl_clndr_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_fscl_clndr') }}
),
final as(
    select 
    *
    from v_ax_fscl_clndr_curr
    where rnk = 1 and delete_dtm is null
)

select * from final
