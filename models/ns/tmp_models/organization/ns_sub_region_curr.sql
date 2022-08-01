with ns_sub_region as(
    select *,
    rank() over(partition by sub_region_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ns_sub_region') }}
),
final as(
    select 
    *
    from ns_sub_region
    where rnk = 1 and delete_dtm is null
)

select * from final