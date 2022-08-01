

with opco_currency as(
    
    
        select 
        'A' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm, 
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX'::varchar as src_sys_nm,
        upper(trim(currency)) as src_currency_cd,
        trim(description) as src_currency_nm
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYA_DBO_TBLCURRENCY
         union all 
    
        select 
        'C' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm, 
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX'::varchar as src_sys_nm,
        upper(trim(currency)) as src_currency_cd,
        trim(description) as src_currency_nm
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYC_DBO_TBLCURRENCY
         union all 
    
        select 
        'P' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm, 
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX'::varchar as src_sys_nm,
        upper(trim(currency)) as src_currency_cd,
        trim(description) as src_currency_nm
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYP_DBO_TBLCURRENCY
         union all 
    
        select 
        'X' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm, 
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX'::varchar as src_sys_nm,
        upper(trim(currency)) as src_currency_cd,
        trim(description) as src_currency_nm
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYX_DBO_TBLCURRENCY
        
    
),
de_duplication as(
    select *
           , rank() over(partition by src_currency_cd order by stg_load_dtm, src_comp) as rnk
    from opco_currency
),
final as(
    select distinct
            md5(cast(coalesce(cast(src_sys_nm as 
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
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_currency_cd, src_currency_nm
    from de_duplication
    where rnk = 1
),
delete_captured as (
    select * from final
    
        union
        select
        OPCO_CURRENCY_SK, OPCO_CURRENCY_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        SRC_SYS_NM, SRC_CURRENCY_CD, SRC_CURRENCY_NM  
        from OI_DATA_DEV_V2.FND_DEV_BKP.opco_currency
        
            where opco_currency_ck not in (select distinct opco_currency_ck from final)
         
        and src_sys_nm = 'SYSPRO-PRMX'
    
)

select * from delete_captured



    
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_currency ) and opco_currency_ck not in (select distinct opco_currency_ck from OI_DATA_DEV_V2.FND_DEV_BKP.opco_currency where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_currency ) and opco_currency_ck in (select distinct opco_currency_ck from OI_DATA_DEV_V2.FND_DEV_BKP.opco_currency where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    
