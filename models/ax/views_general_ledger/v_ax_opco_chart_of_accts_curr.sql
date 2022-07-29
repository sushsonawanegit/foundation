with v_ax_opco_chart_of_accts_curr as(
    select *,
    rank() over(partition by opco_chart_of_accts_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_opco_chart_of_accts') }}
),
final as(
    select 
    *
    from v_ax_opco_chart_of_accts_curr
    where rnk = 1 and delete_dtm is null
)

select * from final
