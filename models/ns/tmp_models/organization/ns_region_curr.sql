with ns_region as(
    select *,
    rank() over(partition by region_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ns_region') }}
),
final as(
    select 
    *
    from ns_region
    where rnk = 1 and delete_dtm is null
)

select * from final