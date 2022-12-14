

with jde_opco_currency as(
    select 
    current_timestamp as crt_dtm,
    null::timestamp_tz as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'JDE-HNCK'::varchar(20) as src_sys_nm,
    upper(trim(cvcrcd))::varchar(10) as src_currency_cd,
    trim(cvdl01)::varchar(50) as src_currency_nm
    from MULE_STAGING.JDE_HANCOCK.JDE_PRODUCTION_PRODDTA_F0013
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_currency_cd as 
    varchar
), '') as 
    varchar
)) as opco_currency_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_currency_cd as 
    varchar
), '') || '-' || coalesce(cast(src_currency_nm as 
    varchar
), '') as 
    varchar
)) as opco_currency_ck,
            *
    from jde_opco_currency
),
delete_captured as(
    select * from final
    
        union
        select
        OPCO_CURRENCY_SK, OPCO_CURRENCY_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        SRC_SYS_NM, SRC_CURRENCY_CD, SRC_CURRENCY_NM  
        from OI_DATA_DEV_V2.FND_DEV_BKP.opco_currency
        
            where opco_currency_ck not in (select distinct opco_currency_ck from final)
         
        and src_sys_nm = 'JDE-HNCK'
    
)

select * from delete_captured



    
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_currency ) and opco_currency_ck not in (select distinct opco_currency_ck from OI_DATA_DEV_V2.FND_DEV_BKP.opco_currency where src_sys_nm = 'JDE-HNCK')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_currency ) and opco_currency_ck in (select distinct opco_currency_ck from OI_DATA_DEV_V2.FND_DEV_BKP.opco_currency where src_sys_nm = 'JDE-HNCK') and delete_dtm is not null)
    
