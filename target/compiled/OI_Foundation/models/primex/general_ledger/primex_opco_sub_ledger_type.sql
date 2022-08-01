

with opco_sub_ledger_type as(
    
    
        select
        'A' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(type)) as src_sub_ledger_type_cd,
        case 
            when trim(type) = 'BIL' then 'Billing'
            when trim(type) = 'CASH' then 'Cash'
            when trim(type) = 'CSBK' then 'Cashbook'
            when trim(type) = 'DBS' then 'Disbursement'
            when trim(type) = 'EXP' then 'Expense'
            when trim(type) = 'GRN' then 'Goods Receive Note'
            when trim(type) = 'INV' then 'Inventory'
            when trim(type) = 'LAB' then 'Labour'
            when trim(type) = 'SALE' then 'Sales'
            else trim(type)  
        end as src_sub_ledger_type_desc,
        1::number(1,0) as actv_ind
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYA_DBO_GENJOURNALDETAIL
         union all 
    
        select
        'C' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(type)) as src_sub_ledger_type_cd,
        case 
            when trim(type) = 'BIL' then 'Billing'
            when trim(type) = 'CASH' then 'Cash'
            when trim(type) = 'CSBK' then 'Cashbook'
            when trim(type) = 'DBS' then 'Disbursement'
            when trim(type) = 'EXP' then 'Expense'
            when trim(type) = 'GRN' then 'Goods Receive Note'
            when trim(type) = 'INV' then 'Inventory'
            when trim(type) = 'LAB' then 'Labour'
            when trim(type) = 'SALE' then 'Sales'
            else trim(type)  
        end as src_sub_ledger_type_desc,
        1::number(1,0) as actv_ind
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYC_DBO_GENJOURNALDETAIL
         union all 
    
        select
        'P' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(type)) as src_sub_ledger_type_cd,
        case 
            when trim(type) = 'BIL' then 'Billing'
            when trim(type) = 'CASH' then 'Cash'
            when trim(type) = 'CSBK' then 'Cashbook'
            when trim(type) = 'DBS' then 'Disbursement'
            when trim(type) = 'EXP' then 'Expense'
            when trim(type) = 'GRN' then 'Goods Receive Note'
            when trim(type) = 'INV' then 'Inventory'
            when trim(type) = 'LAB' then 'Labour'
            when trim(type) = 'SALE' then 'Sales'
            else trim(type)  
        end as src_sub_ledger_type_desc,
        1::number(1,0) as actv_ind
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYP_DBO_GENJOURNALDETAIL
         union all 
    
        select
        'X' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(type)) as src_sub_ledger_type_cd,
        case 
            when trim(type) = 'BIL' then 'Billing'
            when trim(type) = 'CASH' then 'Cash'
            when trim(type) = 'CSBK' then 'Cashbook'
            when trim(type) = 'DBS' then 'Disbursement'
            when trim(type) = 'EXP' then 'Expense'
            when trim(type) = 'GRN' then 'Goods Receive Note'
            when trim(type) = 'INV' then 'Inventory'
            when trim(type) = 'LAB' then 'Labour'
            when trim(type) = 'SALE' then 'Sales'
            else trim(type)  
        end as src_sub_ledger_type_desc,
        1::number(1,0) as actv_ind
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYX_DBO_GENJOURNALDETAIL
        
    
),
de_duplication as(
    select *
           , rank() over(partition by src_sub_ledger_type_cd order by src_comp) as rnk
    from opco_sub_ledger_type
),
final as(
    select distinct
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_sub_ledger_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_sub_ledger_type_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_sub_ledger_type_cd as 
    varchar
), '') || '-' || coalesce(cast(src_sub_ledger_type_desc as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') as 
    varchar
)) as opco_sub_ledger_type_ck,
            crt_dtm,stg_load_dtm, delete_dtm, src_sys_nm, src_sub_ledger_type_cd, src_sub_ledger_type_desc, actv_ind            
    from de_duplication
    where rnk = 1 
),
delete_captured as(
    select * from final
    
        union
        select
        OPCO_SUB_LEDGER_TYPE_SK, OPCO_SUB_LEDGER_TYPE_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        SRC_SYS_NM, src_sub_ledger_type_cd, src_sub_ledger_type_desc, actv_ind
        from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_SUB_LEDGER_TYPE
        
            where OPCO_SUB_LEDGER_TYPE_ck not in (select distinct OPCO_SUB_LEDGER_TYPE_ck from final)
         
        and src_sys_nm = 'SYSPRO-PRMX'
    
)

select * from delete_captured



    
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_sub_ledger_type ) and OPCO_SUB_LEDGER_TYPE_ck not in (select distinct OPCO_SUB_LEDGER_TYPE_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_SUB_LEDGER_TYPE where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_sub_ledger_type ) and OPCO_SUB_LEDGER_TYPE_ck in (select distinct OPCO_SUB_LEDGER_TYPE_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_SUB_LEDGER_TYPE where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    
