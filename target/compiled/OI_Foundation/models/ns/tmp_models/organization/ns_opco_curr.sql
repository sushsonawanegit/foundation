with ns_opco as(
    select *,
    rank() over(partition by opco_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco
),
final as(
    select 
    *
    from ns_opco
    where rnk = 1 and delete_dtm is null
)

select * from final