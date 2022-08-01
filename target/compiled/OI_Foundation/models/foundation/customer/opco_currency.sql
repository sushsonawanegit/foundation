with opco_currency as(
    select *,
    rank() over(partition by opco_currency_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_currency_lineage
),
final as(
    select 
    OPCO_CURRENCY_SK, OPCO_CURRENCY_CK, CRT_DTM, STG_LOAD_DTM, DELETE_DTM, SRC_SYS_NM, SRC_CURRENCY_CD, SRC_CURRENCY_NM
    from opco_currency
    where rnk = 1 and delete_dtm is null
)

select * from final