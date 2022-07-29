with v_ax_region_curr as(
    select *,
    rank() over(partition by region_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_region') }}
),
final as(
    select 
    *
    from v_ax_region_curr
    where rnk = 1 and delete_dtm is null
)

select * from final