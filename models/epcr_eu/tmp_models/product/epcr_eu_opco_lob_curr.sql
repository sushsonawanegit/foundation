with epcr_eu_opco_lob as(
    select *,
    rank() over(partition by opco_lob_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('epcr_eu_opco_lob') }}
),
final as(
    select 
    *
    from epcr_eu_opco_lob
    where rnk = 1 and delete_dtm is null
)

select * from final