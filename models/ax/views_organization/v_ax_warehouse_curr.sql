with v_ax_warehouse_curr as(
    select *,
    rank() over(partition by warehouse_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_warehouse') }}
),
final as(
    select 
    *
    from v_ax_warehouse_curr
    where rnk = 1 and delete_dtm is null
)

select * from final
