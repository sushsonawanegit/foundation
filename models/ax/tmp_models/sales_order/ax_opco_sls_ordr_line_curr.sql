with ax_opco_sls_ordr_line as(
    select *,
    rank() over(partition by opco_sls_ordr_line_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_opco_sls_ordr_line') }}
),
final as(
    select 
    *
    from ax_opco_sls_ordr_line
    where rnk = 1 and delete_dtm is null
)

select * from final

