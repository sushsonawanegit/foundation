with ns_opco_brand as(
    select *,
    rank() over(partition by opco_brand_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_brand
),
final as(
    select 
    *
    from ns_opco_brand
    where rnk = 1 and delete_dtm is null
)

select * from final