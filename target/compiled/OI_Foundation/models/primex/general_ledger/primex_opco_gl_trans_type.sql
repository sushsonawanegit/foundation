

with primex_opco_gl_trans_type as(
    
    
        select
        'A' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(journalsource)) as src_gl_trans_type_cd,
        case 
            when trim(journalsource) = 'AP' then 'Accounts Payable'
            when trim(journalsource) = 'AR' then 'Accounts Receivable'
            when trim(journalsource) = 'CS' then 'Cash'
            when trim(journalsource) = 'GR' then 'Goods Receipt'
            when trim(journalsource) = 'IN' then 'Inventory'
            when trim(journalsource) = 'SA' then 'Sales'
            when trim(journalsource) = 'WP' then 'WIP Labour'
            else trim(journalsource)
        end as src_gl_trans_type_desc
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYA_DBO_GENJOURNALCTL
         union all 
    
        select
        'C' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(journalsource)) as src_gl_trans_type_cd,
        case 
            when trim(journalsource) = 'AP' then 'Accounts Payable'
            when trim(journalsource) = 'AR' then 'Accounts Receivable'
            when trim(journalsource) = 'CS' then 'Cash'
            when trim(journalsource) = 'GR' then 'Goods Receipt'
            when trim(journalsource) = 'IN' then 'Inventory'
            when trim(journalsource) = 'SA' then 'Sales'
            when trim(journalsource) = 'WP' then 'WIP Labour'
            else trim(journalsource)
        end as src_gl_trans_type_desc
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYC_DBO_GENJOURNALCTL
         union all 
    
        select
        'P' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(journalsource)) as src_gl_trans_type_cd,
        case 
            when trim(journalsource) = 'AP' then 'Accounts Payable'
            when trim(journalsource) = 'AR' then 'Accounts Receivable'
            when trim(journalsource) = 'CS' then 'Cash'
            when trim(journalsource) = 'GR' then 'Goods Receipt'
            when trim(journalsource) = 'IN' then 'Inventory'
            when trim(journalsource) = 'SA' then 'Sales'
            when trim(journalsource) = 'WP' then 'WIP Labour'
            else trim(journalsource)
        end as src_gl_trans_type_desc
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYP_DBO_GENJOURNALCTL
         union all 
    
        select
        'X' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(journalsource)) as src_gl_trans_type_cd,
        case 
            when trim(journalsource) = 'AP' then 'Accounts Payable'
            when trim(journalsource) = 'AR' then 'Accounts Receivable'
            when trim(journalsource) = 'CS' then 'Cash'
            when trim(journalsource) = 'GR' then 'Goods Receipt'
            when trim(journalsource) = 'IN' then 'Inventory'
            when trim(journalsource) = 'SA' then 'Sales'
            when trim(journalsource) = 'WP' then 'WIP Labour'
            else trim(journalsource)
        end as src_gl_trans_type_desc
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYX_DBO_GENJOURNALCTL
        
    
),
de_duplication as(
    select *
           , rank() over(partition by src_gl_trans_type_cd order by stg_load_dtm, src_comp) as rnk
    from primex_opco_gl_trans_type
),
final as(
    select distinct
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_gl_trans_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_type_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_gl_trans_type_cd as 
    varchar
), '') || '-' || coalesce(cast(src_gl_trans_type_desc as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_type_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_gl_trans_type_cd, src_gl_trans_type_desc
    from de_duplication
    where rnk = 1 
),

delete_captured as(
    select * from final
    
        union
        select
        OPCO_GL_TRANS_TYPE_SK, OPCO_GL_TRANS_TYPE_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        SRC_SYS_NM, src_gl_trans_type_cd, src_gl_trans_type_desc  
        from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_GL_TRANS_TYPE
        
            where OPCO_GL_TRANS_TYPE_ck not in (select distinct OPCO_GL_TRANS_TYPE_ck from final)
         
        and src_sys_nm = 'SYSPRO-PRMX'
    
)

select * from delete_captured



    
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_gl_trans_type ) and OPCO_GL_TRANS_TYPE_ck not in (select distinct OPCO_GL_TRANS_TYPE_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_GL_TRANS_TYPE where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_gl_trans_type ) and OPCO_GL_TRANS_TYPE_ck in (select distinct OPCO_GL_TRANS_TYPE_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_GL_TRANS_TYPE where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    
