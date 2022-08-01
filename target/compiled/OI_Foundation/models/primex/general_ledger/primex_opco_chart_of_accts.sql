-- depends_on: OI_DATA_DEV_V2.FND_DEV_BKP.chart_of_accts
-- depends_on: __dbt__cte__v_primex_opco_cost_center_curr
-- depends_on: __dbt__cte__v_primex_opco_type_curr
-- depends_on: __dbt__cte__v_primex_opco_dept_curr
-- depends_on: __dbt__cte__v_primex_opco_purpose_curr
-- depends_on: __dbt__cte__v_primex_opco_lob_curr
-- depends_on: __dbt__cte__v_primex_opco_chart_of_accts_type_curr



with  __dbt__cte__v_primex_opco_cost_center_curr as (
with v_primex_opco_cost_center as(
    select *,
    rank() over(partition by opco_cost_center_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_cost_center
),
final as(
    select 
    *
    from v_primex_opco_cost_center
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_primex_opco_type_curr as (
with v_primex_opco_type as(
    select *,
    rank() over(partition by opco_type_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_type
),
final as(
    select 
    *
    from v_primex_opco_type
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_primex_opco_dept_curr as (
with v_primex_opco_dept as(
    select *,
    rank() over(partition by opco_dept_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_dept
),
final as(
    select 
    *
    from v_primex_opco_dept
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_primex_opco_purpose_curr as (
with v_primex_opco_purpose as(
    select *,
    rank() over(partition by opco_purpose_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_purpose
),
final as(
    select 
    *
    from v_primex_opco_purpose
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_primex_opco_lob_curr as (
with v_primex_opco_lob as(
    select *,
    rank() over(partition by opco_lob_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_lob
),
final as(
    select 
    *
    from v_primex_opco_lob
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_primex_opco_chart_of_accts_type_curr as (
with v_primex_opco_chart_of_accts_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_chart_of_accts_type
)

select * from v_primex_opco_chart_of_accts_type
),primex_opco_chart_of_accts as(
    
    
        select
        'A' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(cdg.glcode)) as gl_acct_nbr,
        upper(trim(cdg.company)) as opco_id,
        trim(cdg.description) as gl_acct_nm,
        coa.chart_of_accts_sk,
        occ.opco_cost_center_sk,
        od.opco_dept_sk,
        ot.opco_type_sk,
        oip.opco_purpose_sk,
        ol.opco_lob_sk,
        ocoa.opco_chart_of_accts_type_sk,
        null as opco_uom_sk,
        null as gl_acct_alias_nm,
        iff(trim(cdg.controlaccflag) = 'Y', 0, 1) as manual_entry_allowed_ind,
        null as dpo_exclusion_ind,
        null as movement_allowed_ind,
        null as cost_center_reqd_txt,
        null as dept_reqd_txt,
        null as type_reqd_txt,
        null as prnt_gl_acct_nbr,
        null as related_gl_acct_nbr,
        null as monetary_gl_acct_ind,
        iff(trim(cdg.acconhldflag) in ('', 'N'), 1 , 0) as actv_ind,
        iff(trim(cdc.financialstatement) = 'BS', 1, 0) as balance_sheet_acct_ind,
        null as summary_acct_ind,
        null as opco_brand_sk,
        null as acct_clssfctn_cd,
        null as sub_acct_nbr
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYA_DBO_GENMASTER cdg
        left join MULE_STAGING.SYSPRO_PRIMEX.COMPANYA_DBO_CUSGENMASTER_ cdc 
            on trim(cdg.glcode) = trim(cdc.glcode)
        left join OI_DATA_DEV_V2.FND_DEV_BKP.chart_of_accts coa 
            on coa.gl_acct_nbr = trim(cdc.gl)
        left join __dbt__cte__v_primex_opco_cost_center_curr occ 
            on occ.src_cost_center_cd = upper(trim(cdc.cc))
            and occ.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_dept_curr od 
            on od.src_dept_cd = upper(trim(cdc.dept))
            and od.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_type_curr ot 
            on ot.src_type_cd = upper(trim(cdc.type))
            and ot.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_purpose_curr oip 
            on oip.src_purpose_cd = upper(trim(cdc.purpose))
            and oip.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_lob_curr ol 
            on ol.src_lob_cd = upper(trim(cdc.fsgroup4buname))
            and ol.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_chart_of_accts_type_curr ocoa 
            on ocoa.src_acct_type_cd = upper(trim(cdg.glgroup))
            and ocoa.src_sys_nm = 'SYSPRO-PRMX'
        where upper(trim(cdg.company)) = 'A'
         union all 
    
        select
        'C' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(cdg.glcode)) as gl_acct_nbr,
        upper(trim(cdg.company)) as opco_id,
        trim(cdg.description) as gl_acct_nm,
        coa.chart_of_accts_sk,
        occ.opco_cost_center_sk,
        od.opco_dept_sk,
        ot.opco_type_sk,
        oip.opco_purpose_sk,
        ol.opco_lob_sk,
        ocoa.opco_chart_of_accts_type_sk,
        null as opco_uom_sk,
        null as gl_acct_alias_nm,
        iff(trim(cdg.controlaccflag) = 'Y', 0, 1) as manual_entry_allowed_ind,
        null as dpo_exclusion_ind,
        null as movement_allowed_ind,
        null as cost_center_reqd_txt,
        null as dept_reqd_txt,
        null as type_reqd_txt,
        null as prnt_gl_acct_nbr,
        null as related_gl_acct_nbr,
        null as monetary_gl_acct_ind,
        iff(trim(cdg.acconhldflag) in ('', 'N'), 1 , 0) as actv_ind,
        iff(trim(cdc.financialstatement) = 'BS', 1, 0) as balance_sheet_acct_ind,
        null as summary_acct_ind,
        null as opco_brand_sk,
        null as acct_clssfctn_cd,
        null as sub_acct_nbr
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYC_DBO_GENMASTER cdg
        left join MULE_STAGING.SYSPRO_PRIMEX.COMPANYC_DBO_CUSGENMASTER_ cdc 
            on trim(cdg.glcode) = trim(cdc.glcode)
        left join OI_DATA_DEV_V2.FND_DEV_BKP.chart_of_accts coa 
            on coa.gl_acct_nbr = trim(cdc.gl)
        left join __dbt__cte__v_primex_opco_cost_center_curr occ 
            on occ.src_cost_center_cd = upper(trim(cdc.cc))
            and occ.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_dept_curr od 
            on od.src_dept_cd = upper(trim(cdc.dept))
            and od.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_type_curr ot 
            on ot.src_type_cd = upper(trim(cdc.type))
            and ot.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_purpose_curr oip 
            on oip.src_purpose_cd = upper(trim(cdc.purpose))
            and oip.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_lob_curr ol 
            on ol.src_lob_cd = upper(trim(cdc.fsgroup4buname))
            and ol.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_chart_of_accts_type_curr ocoa 
            on ocoa.src_acct_type_cd = upper(trim(cdg.glgroup))
            and ocoa.src_sys_nm = 'SYSPRO-PRMX'
        where upper(trim(cdg.company)) = 'C'
         union all 
    
        select
        'P' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(cdg.glcode)) as gl_acct_nbr,
        upper(trim(cdg.company)) as opco_id,
        trim(cdg.description) as gl_acct_nm,
        coa.chart_of_accts_sk,
        occ.opco_cost_center_sk,
        od.opco_dept_sk,
        ot.opco_type_sk,
        oip.opco_purpose_sk,
        ol.opco_lob_sk,
        ocoa.opco_chart_of_accts_type_sk,
        null as opco_uom_sk,
        null as gl_acct_alias_nm,
        iff(trim(cdg.controlaccflag) = 'Y', 0, 1) as manual_entry_allowed_ind,
        null as dpo_exclusion_ind,
        null as movement_allowed_ind,
        null as cost_center_reqd_txt,
        null as dept_reqd_txt,
        null as type_reqd_txt,
        null as prnt_gl_acct_nbr,
        null as related_gl_acct_nbr,
        null as monetary_gl_acct_ind,
        iff(trim(cdg.acconhldflag) in ('', 'N'), 1 , 0) as actv_ind,
        iff(trim(cdc.financialstatement) = 'BS', 1, 0) as balance_sheet_acct_ind,
        null as summary_acct_ind,
        null as opco_brand_sk,
        null as acct_clssfctn_cd,
        null as sub_acct_nbr
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYP_DBO_GENMASTER cdg
        left join MULE_STAGING.SYSPRO_PRIMEX.COMPANYP_DBO_CUSGENMASTER_ cdc 
            on trim(cdg.glcode) = trim(cdc.glcode)
        left join OI_DATA_DEV_V2.FND_DEV_BKP.chart_of_accts coa 
            on coa.gl_acct_nbr = trim(cdc.gl)
        left join __dbt__cte__v_primex_opco_cost_center_curr occ 
            on occ.src_cost_center_cd = upper(trim(cdc.cc))
            and occ.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_dept_curr od 
            on od.src_dept_cd = upper(trim(cdc.dept))
            and od.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_type_curr ot 
            on ot.src_type_cd = upper(trim(cdc.type))
            and ot.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_purpose_curr oip 
            on oip.src_purpose_cd = upper(trim(cdc.purpose))
            and oip.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_lob_curr ol 
            on ol.src_lob_cd = upper(trim(cdc.fsgroup4buname))
            and ol.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_chart_of_accts_type_curr ocoa 
            on ocoa.src_acct_type_cd = upper(trim(cdg.glgroup))
            and ocoa.src_sys_nm = 'SYSPRO-PRMX'
        where upper(trim(cdg.company)) = 'P'
         union all 
    
        select
        'X' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(cdg.glcode)) as gl_acct_nbr,
        upper(trim(cdg.company)) as opco_id,
        trim(cdg.description) as gl_acct_nm,
        coa.chart_of_accts_sk,
        occ.opco_cost_center_sk,
        od.opco_dept_sk,
        ot.opco_type_sk,
        oip.opco_purpose_sk,
        ol.opco_lob_sk,
        ocoa.opco_chart_of_accts_type_sk,
        null as opco_uom_sk,
        null as gl_acct_alias_nm,
        iff(trim(cdg.controlaccflag) = 'Y', 0, 1) as manual_entry_allowed_ind,
        null as dpo_exclusion_ind,
        null as movement_allowed_ind,
        null as cost_center_reqd_txt,
        null as dept_reqd_txt,
        null as type_reqd_txt,
        null as prnt_gl_acct_nbr,
        null as related_gl_acct_nbr,
        null as monetary_gl_acct_ind,
        iff(trim(cdg.acconhldflag) in ('', 'N'), 1 , 0) as actv_ind,
        iff(trim(cdc.financialstatement) = 'BS', 1, 0) as balance_sheet_acct_ind,
        null as summary_acct_ind,
        null as opco_brand_sk,
        null as acct_clssfctn_cd,
        null as sub_acct_nbr
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYX_DBO_GENMASTER cdg
        left join MULE_STAGING.SYSPRO_PRIMEX.COMPANYX_DBO_CUSGENMASTER_ cdc 
            on trim(cdg.glcode) = trim(cdc.glcode)
        left join OI_DATA_DEV_V2.FND_DEV_BKP.chart_of_accts coa 
            on coa.gl_acct_nbr = trim(cdc.gl)
        left join __dbt__cte__v_primex_opco_cost_center_curr occ 
            on occ.src_cost_center_cd = upper(trim(cdc.cc))
            and occ.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_dept_curr od 
            on od.src_dept_cd = upper(trim(cdc.dept))
            and od.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_type_curr ot 
            on ot.src_type_cd = upper(trim(cdc.type))
            and ot.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_purpose_curr oip 
            on oip.src_purpose_cd = upper(trim(cdc.purpose))
            and oip.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_lob_curr ol 
            on ol.src_lob_cd = upper(trim(cdc.fsgroup4buname))
            and ol.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_chart_of_accts_type_curr ocoa 
            on ocoa.src_acct_type_cd = upper(trim(cdg.glgroup))
            and ocoa.src_sys_nm = 'SYSPRO-PRMX'
        where upper(trim(cdg.company)) = 'X'
        
    
),
de_duplication as(
    select *
           , rank() over(partition by opco_id, gl_acct_nbr order by stg_load_dtm, src_comp) as rnk
    from primex_opco_chart_of_accts
),
final as(
    select distinct
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(gl_acct_nbr as 
    varchar
), '') as 
    varchar
)) as opco_chart_of_accts_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(gl_acct_nbr as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(gl_acct_nm as 
    varchar
), '') || '-' || coalesce(cast(chart_of_accts_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cost_center_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dept_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_purpose_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_lob_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_chart_of_accts_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(GL_ACCT_ALIAS_NM as 
    varchar
), '') || '-' || coalesce(cast(manual_entry_allowed_ind as 
    varchar
), '') || '-' || coalesce(cast(dpo_exclusion_ind as 
    varchar
), '') || '-' || coalesce(cast(movement_allowed_ind as 
    varchar
), '') || '-' || coalesce(cast(cost_center_reqd_txt as 
    varchar
), '') || '-' || coalesce(cast(dept_reqd_txt as 
    varchar
), '') || '-' || coalesce(cast(type_reqd_txt as 
    varchar
), '') || '-' || coalesce(cast(prnt_gl_acct_nbr as 
    varchar
), '') || '-' || coalesce(cast(related_gl_acct_nbr as 
    varchar
), '') || '-' || coalesce(cast(monetary_gl_acct_ind as 
    varchar
), '') || '-' || coalesce(cast(balance_sheet_acct_ind as 
    varchar
), '') || '-' || coalesce(cast(summary_acct_ind as 
    varchar
), '') || '-' || coalesce(cast(OPCO_BRAND_SK as 
    varchar
), '') || '-' || coalesce(cast(acct_clssfctn_cd as 
    varchar
), '') || '-' || coalesce(cast(sub_acct_nbr as 
    varchar
), '') as 
    varchar
)) as opco_chart_of_accts_ck,
            CRT_DTM, STG_LOAD_DTM, DELETE_DTM, SRC_SYS_NM, GL_ACCT_NBR, OPCO_ID, GL_ACCT_NM, CHART_OF_ACCTS_SK, OPCO_COST_CENTER_SK, OPCO_DEPT_SK, OPCO_TYPE_SK, OPCO_PURPOSE_SK, OPCO_LOB_SK, OPCO_CHART_OF_ACCTS_TYPE_SK, OPCO_UOM_SK, GL_ACCT_ALIAS_NM, MANUAL_ENTRY_ALLOWED_IND, DPO_EXCLUSION_IND, MOVEMENT_ALLOWED_IND, COST_CENTER_REQD_TXT, DEPT_REQD_TXT, TYPE_REQD_TXT, PRNT_GL_ACCT_NBR, RELATED_GL_ACCT_NBR, MONETARY_GL_ACCT_IND, ACTV_IND, BALANCE_SHEET_ACCT_IND, SUMMARY_ACCT_IND, OPCO_BRAND_SK, ACCT_CLSSFCTN_CD, SUB_ACCT_NBR
    from de_duplication
    where rnk = 1 
),
delete_captured as(
    select * from final
    
        union
        select
        OPCO_CHART_OF_ACCTS_sk, OPCO_CHART_OF_ACCTS_ck, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        SRC_SYS_NM, GL_ACCT_NBR, OPCO_ID, GL_ACCT_NM, CHART_OF_ACCTS_SK, OPCO_COST_CENTER_SK, OPCO_DEPT_SK, OPCO_TYPE_SK, OPCO_PURPOSE_SK, OPCO_LOB_SK, OPCO_CHART_OF_ACCTS_TYPE_SK, OPCO_UOM_SK, GL_ACCT_ALIAS_NM, MANUAL_ENTRY_ALLOWED_IND, DPO_EXCLUSION_IND, MOVEMENT_ALLOWED_IND, COST_CENTER_REQD_TXT, DEPT_REQD_TXT, TYPE_REQD_TXT, PRNT_GL_ACCT_NBR, RELATED_GL_ACCT_NBR, MONETARY_GL_ACCT_IND, ACTV_IND, BALANCE_SHEET_ACCT_IND, SUMMARY_ACCT_IND, OPCO_BRAND_SK, ACCT_CLSSFCTN_CD, SUB_ACCT_NBR
        from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_CHART_OF_ACCTS
        
            where OPCO_CHART_OF_ACCTS_ck not in (select distinct OPCO_CHART_OF_ACCTS_ck from final)
         
        and src_sys_nm = 'SYSPRO-PRMX'
    
)

select * from delete_captured



    
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_chart_of_accts ) and OPCO_CHART_OF_ACCTS_ck not in (select distinct OPCO_CHART_OF_ACCTS_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_CHART_OF_ACCTS where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_chart_of_accts ) and OPCO_CHART_OF_ACCTS_ck in (select distinct OPCO_CHART_OF_ACCTS_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_CHART_OF_ACCTS where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    
