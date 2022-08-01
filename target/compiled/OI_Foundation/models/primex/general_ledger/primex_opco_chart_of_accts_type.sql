

with primex_opco_chart_of_accts_type as(
    
    
        select
        'A' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(glgroup)) as src_acct_type_cd,
        trim(description) as src_acct_type_desc
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYA_DBO_GENGROUPS
         union all 
    
        select
        'C' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(glgroup)) as src_acct_type_cd,
        trim(description) as src_acct_type_desc
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYC_DBO_GENGROUPS
         union all 
    
        select
        'P' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(glgroup)) as src_acct_type_cd,
        trim(description) as src_acct_type_desc
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYP_DBO_GENGROUPS
         union all 
    
        select
        'X' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(glgroup)) as src_acct_type_cd,
        trim(description) as src_acct_type_desc
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYX_DBO_GENGROUPS
        
    
),
de_duplication as(
    select *
           , rank() over(partition by src_acct_type_cd order by src_comp) as rnk
    from primex_opco_chart_of_accts_type
),
final as(
    select distinct
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_acct_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_chart_of_accts_type_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_acct_type_cd as 
    varchar
), '') || '-' || coalesce(cast(src_acct_type_desc as 
    varchar
), '') as 
    varchar
)) as opco_chart_of_accts_type_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_acct_type_cd, src_acct_type_desc       
    from de_duplication
    where rnk = 1 
),
delete_captured as(
    select * from final
    
        union
        select
        OPCO_CHART_OF_ACCTS_TYPE_SK, OPCO_CHART_OF_ACCTS_TYPE_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, src_acct_type_cd, src_acct_type_desc   
        from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_CHART_OF_ACCTS_TYPE
        
            where OPCO_CHART_OF_ACCTS_TYPE_ck not in (select distinct OPCO_CHART_OF_ACCTS_TYPE_ck from final)
         
        and src_sys_nm = 'SYSPRO-PRMX'
    
)

select * from delete_captured



    
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_chart_of_accts_type ) and OPCO_CHART_OF_ACCTS_TYPE_ck not in (select distinct OPCO_CHART_OF_ACCTS_TYPE_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_CHART_OF_ACCTS_TYPE where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_chart_of_accts_type ) and OPCO_CHART_OF_ACCTS_TYPE_ck in (select distinct OPCO_CHART_OF_ACCTS_TYPE_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_CHART_OF_ACCTS_TYPE where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    
