with ax_opco_dept as(
    select *,
    rank() over(partition by opco_dept_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_opco_dept') }}
),
final as(
    select 
    *
    from ax_opco_dept
    where rnk = 1 and delete_dtm is null
)

select * from final

