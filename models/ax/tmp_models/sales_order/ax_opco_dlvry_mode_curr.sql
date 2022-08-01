with ax_opco_dlvry_mode as(
    select *,
    rank() over(partition by opco_dlvry_mode_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_opco_dlvry_mode') }}
),
final as(
    select 
    *
    from ax_opco_dlvry_mode
    where rnk = 1 and delete_dtm is null
)

select * from final

