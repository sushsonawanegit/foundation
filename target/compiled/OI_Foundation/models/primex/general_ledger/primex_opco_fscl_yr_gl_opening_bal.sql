-- depends_on: __dbt__cte__v_primex_opco_curr
-- depends_on: __dbt__cte__v_primex_opco_chart_of_accts_curr
-- depends_on: __dbt__cte__v_primex_opco_cost_center_curr
-- depends_on: __dbt__cte__v_primex_opco_type_curr
-- depends_on: __dbt__cte__v_primex_opco_dept_curr
-- depends_on: __dbt__cte__v_primex_opco_purpose_curr
-- depends_on: __dbt__cte__v_primex_opco_lob_curr
-- depends_on: OI_DATA_DEV_V2.FND_DEV_BKP.fscl_clndr



with  __dbt__cte__v_primex_opco_curr as (
with v_primex_opco as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco
)

select * from v_primex_opco
),  __dbt__cte__v_primex_opco_chart_of_accts_curr as (
with v_primex_opco_chart_of_accts as(
    select *,
    rank() over(partition by opco_chart_of_accts_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_chart_of_accts
),
final as(
    select 
    *
    from v_primex_opco_chart_of_accts
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_primex_opco_cost_center_curr as (
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
),primex_opco_fscl_yr_gl_opening_bal as (
   
   
      select 
      'A' as src_comp,
      current_timestamp as crt_dtm,
      null::timestamp_tz as stg_load_dtm,
      null::timestamp_tz as delete_dtm,
      md5(cast(coalesce(cast('SYSPRO-PRMX' as 
    varchar
), '') || '-' || coalesce(cast(ifnull(trim(gh.company), '{{tbl}}') as 
    varchar
), '') || '-' || coalesce(cast(trim(gh.glyear) as 
    varchar
), '') || '-' || coalesce(cast(trim(gh.glcode) as 
    varchar
), '') as 
    varchar
)) as opco_fscl_yr_gl_opening_bal_sk, 
      concat_ws('|', 'SYSPRO-PRMX', ifnull(trim(gh.company), 'A'), trim(gh.glyear), trim(gh.glcode)) as opco_fscl_yr_gl_opening_bal_ak,
      'SYSPRO-PRMX' as src_sys_nm,
      null as src_key_txt,
      opco.opco_sk,
      ocoa.opco_chart_of_accts_sk,
      null as opco_gl_trans_type_sk,
      null as opco_gl_trans_posting_type_sk,
      occ.opco_cost_center_sk,
      od.opco_dept_sk,
      ot.opco_type_sk,
      op.opco_purpose_sk,
      ol.opco_lob_sk,
      opco.opco_currency_sk as trans_currency_sk,
      case 
         when trim(gh.glyear) >= 2020 then fc.fscl_yr_strt_dt
         when trim(gh.glyear) < 2020 then concat(trim(gh.glyear), '-04-01')::date
      end as fscl_yr_strt_dt,
      trim(gh.glyear) as fscl_yr_nbr,
      null as credit_ind,
      null as opening_bal_qty,
      trim(gh.beginyearbalance) as trans_currency_opening_bal_amt,
      trim(gh.beginyearbalance) as opco_currency_opening_bal_amt,
      null as opco_uom_sk,
      null as opco_brand_sk,
      null as opco_sub_ledger_type_sk,
      null as sub_ledger_cd
      from MULE_STAGING.SYSPRO_PRIMEX.COMPANYA_DBO_GENHISTORY as gh
      left join  MULE_STAGING.SYSPRO_PRIMEX.COMPANYA_DBO_CUSGENMASTER_ as cgm
         on gh.glcode=cgm.glcode
      left join __dbt__cte__v_primex_opco_curr opco 
         on opco.opco_id = iff(trim(gh.company) is null, 'A', trim(gh.company))
         and opco.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_chart_of_accts_curr ocoa 
         on ocoa.gl_acct_nbr = trim(gh.glcode)
         and ocoa.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_cost_center_curr occ
         on occ.src_cost_center_cd = upper(trim(cgm.cc)) 
         and occ.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_dept_curr od 
         on od.src_dept_cd = upper(trim(cgm.dept)) 
         and od.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_type_curr ot 
         on ot.src_type_cd = upper(trim(cgm.type)) 
         and ot.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_purpose_curr op 
         on op.src_purpose_cd = upper(trim(cgm.purpose)) 
         and op.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_lob_curr ol 
         on ol.src_lob_cd = upper(trim(cgm.fsgroup4buname)) 
         and ol.src_sys_nm = 'SYSPRO-PRMX' 
      left join OI_DATA_DEV_V2.FND_DEV_BKP.fscl_clndr fc 
         on fc.fscl_yr_id = trim(gh.glyear)
      where trim(gh.glyear) >= 2020 and trim(gh.company) in (null, 'A')
       union all 
   
      select 
      'C' as src_comp,
      current_timestamp as crt_dtm,
      null::timestamp_tz as stg_load_dtm,
      null::timestamp_tz as delete_dtm,
      md5(cast(coalesce(cast('SYSPRO-PRMX' as 
    varchar
), '') || '-' || coalesce(cast(ifnull(trim(gh.company), '{{tbl}}') as 
    varchar
), '') || '-' || coalesce(cast(trim(gh.glyear) as 
    varchar
), '') || '-' || coalesce(cast(trim(gh.glcode) as 
    varchar
), '') as 
    varchar
)) as opco_fscl_yr_gl_opening_bal_sk, 
      concat_ws('|', 'SYSPRO-PRMX', ifnull(trim(gh.company), 'C'), trim(gh.glyear), trim(gh.glcode)) as opco_fscl_yr_gl_opening_bal_ak,
      'SYSPRO-PRMX' as src_sys_nm,
      null as src_key_txt,
      opco.opco_sk,
      ocoa.opco_chart_of_accts_sk,
      null as opco_gl_trans_type_sk,
      null as opco_gl_trans_posting_type_sk,
      occ.opco_cost_center_sk,
      od.opco_dept_sk,
      ot.opco_type_sk,
      op.opco_purpose_sk,
      ol.opco_lob_sk,
      opco.opco_currency_sk as trans_currency_sk,
      case 
         when trim(gh.glyear) >= 2020 then fc.fscl_yr_strt_dt
         when trim(gh.glyear) < 2020 then concat(trim(gh.glyear), '-04-01')::date
      end as fscl_yr_strt_dt,
      trim(gh.glyear) as fscl_yr_nbr,
      null as credit_ind,
      null as opening_bal_qty,
      trim(gh.beginyearbalance) as trans_currency_opening_bal_amt,
      trim(gh.beginyearbalance) as opco_currency_opening_bal_amt,
      null as opco_uom_sk,
      null as opco_brand_sk,
      null as opco_sub_ledger_type_sk,
      null as sub_ledger_cd
      from MULE_STAGING.SYSPRO_PRIMEX.COMPANYC_DBO_GENHISTORY as gh
      left join  MULE_STAGING.SYSPRO_PRIMEX.COMPANYC_DBO_CUSGENMASTER_ as cgm
         on gh.glcode=cgm.glcode
      left join __dbt__cte__v_primex_opco_curr opco 
         on opco.opco_id = iff(trim(gh.company) is null, 'C', trim(gh.company))
         and opco.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_chart_of_accts_curr ocoa 
         on ocoa.gl_acct_nbr = trim(gh.glcode)
         and ocoa.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_cost_center_curr occ
         on occ.src_cost_center_cd = upper(trim(cgm.cc)) 
         and occ.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_dept_curr od 
         on od.src_dept_cd = upper(trim(cgm.dept)) 
         and od.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_type_curr ot 
         on ot.src_type_cd = upper(trim(cgm.type)) 
         and ot.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_purpose_curr op 
         on op.src_purpose_cd = upper(trim(cgm.purpose)) 
         and op.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_lob_curr ol 
         on ol.src_lob_cd = upper(trim(cgm.fsgroup4buname)) 
         and ol.src_sys_nm = 'SYSPRO-PRMX' 
      left join OI_DATA_DEV_V2.FND_DEV_BKP.fscl_clndr fc 
         on fc.fscl_yr_id = trim(gh.glyear)
      where trim(gh.glyear) >= 2020 and trim(gh.company) in (null, 'C')
       union all 
   
      select 
      'P' as src_comp,
      current_timestamp as crt_dtm,
      null::timestamp_tz as stg_load_dtm,
      null::timestamp_tz as delete_dtm,
      md5(cast(coalesce(cast('SYSPRO-PRMX' as 
    varchar
), '') || '-' || coalesce(cast(ifnull(trim(gh.company), '{{tbl}}') as 
    varchar
), '') || '-' || coalesce(cast(trim(gh.glyear) as 
    varchar
), '') || '-' || coalesce(cast(trim(gh.glcode) as 
    varchar
), '') as 
    varchar
)) as opco_fscl_yr_gl_opening_bal_sk, 
      concat_ws('|', 'SYSPRO-PRMX', ifnull(trim(gh.company), 'P'), trim(gh.glyear), trim(gh.glcode)) as opco_fscl_yr_gl_opening_bal_ak,
      'SYSPRO-PRMX' as src_sys_nm,
      null as src_key_txt,
      opco.opco_sk,
      ocoa.opco_chart_of_accts_sk,
      null as opco_gl_trans_type_sk,
      null as opco_gl_trans_posting_type_sk,
      occ.opco_cost_center_sk,
      od.opco_dept_sk,
      ot.opco_type_sk,
      op.opco_purpose_sk,
      ol.opco_lob_sk,
      opco.opco_currency_sk as trans_currency_sk,
      case 
         when trim(gh.glyear) >= 2020 then fc.fscl_yr_strt_dt
         when trim(gh.glyear) < 2020 then concat(trim(gh.glyear), '-04-01')::date
      end as fscl_yr_strt_dt,
      trim(gh.glyear) as fscl_yr_nbr,
      null as credit_ind,
      null as opening_bal_qty,
      trim(gh.beginyearbalance) as trans_currency_opening_bal_amt,
      trim(gh.beginyearbalance) as opco_currency_opening_bal_amt,
      null as opco_uom_sk,
      null as opco_brand_sk,
      null as opco_sub_ledger_type_sk,
      null as sub_ledger_cd
      from MULE_STAGING.SYSPRO_PRIMEX.COMPANYP_DBO_GENHISTORY as gh
      left join  MULE_STAGING.SYSPRO_PRIMEX.COMPANYP_DBO_CUSGENMASTER_ as cgm
         on gh.glcode=cgm.glcode
      left join __dbt__cte__v_primex_opco_curr opco 
         on opco.opco_id = iff(trim(gh.company) is null, 'P', trim(gh.company))
         and opco.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_chart_of_accts_curr ocoa 
         on ocoa.gl_acct_nbr = trim(gh.glcode)
         and ocoa.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_cost_center_curr occ
         on occ.src_cost_center_cd = upper(trim(cgm.cc)) 
         and occ.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_dept_curr od 
         on od.src_dept_cd = upper(trim(cgm.dept)) 
         and od.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_type_curr ot 
         on ot.src_type_cd = upper(trim(cgm.type)) 
         and ot.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_purpose_curr op 
         on op.src_purpose_cd = upper(trim(cgm.purpose)) 
         and op.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_lob_curr ol 
         on ol.src_lob_cd = upper(trim(cgm.fsgroup4buname)) 
         and ol.src_sys_nm = 'SYSPRO-PRMX' 
      left join OI_DATA_DEV_V2.FND_DEV_BKP.fscl_clndr fc 
         on fc.fscl_yr_id = trim(gh.glyear)
      where trim(gh.glyear) >= 2020 and trim(gh.company) in (null, 'P')
       union all 
   
      select 
      'X' as src_comp,
      current_timestamp as crt_dtm,
      null::timestamp_tz as stg_load_dtm,
      null::timestamp_tz as delete_dtm,
      md5(cast(coalesce(cast('SYSPRO-PRMX' as 
    varchar
), '') || '-' || coalesce(cast(ifnull(trim(gh.company), '{{tbl}}') as 
    varchar
), '') || '-' || coalesce(cast(trim(gh.glyear) as 
    varchar
), '') || '-' || coalesce(cast(trim(gh.glcode) as 
    varchar
), '') as 
    varchar
)) as opco_fscl_yr_gl_opening_bal_sk, 
      concat_ws('|', 'SYSPRO-PRMX', ifnull(trim(gh.company), 'X'), trim(gh.glyear), trim(gh.glcode)) as opco_fscl_yr_gl_opening_bal_ak,
      'SYSPRO-PRMX' as src_sys_nm,
      null as src_key_txt,
      opco.opco_sk,
      ocoa.opco_chart_of_accts_sk,
      null as opco_gl_trans_type_sk,
      null as opco_gl_trans_posting_type_sk,
      occ.opco_cost_center_sk,
      od.opco_dept_sk,
      ot.opco_type_sk,
      op.opco_purpose_sk,
      ol.opco_lob_sk,
      opco.opco_currency_sk as trans_currency_sk,
      case 
         when trim(gh.glyear) >= 2020 then fc.fscl_yr_strt_dt
         when trim(gh.glyear) < 2020 then concat(trim(gh.glyear), '-04-01')::date
      end as fscl_yr_strt_dt,
      trim(gh.glyear) as fscl_yr_nbr,
      null as credit_ind,
      null as opening_bal_qty,
      trim(gh.beginyearbalance) as trans_currency_opening_bal_amt,
      trim(gh.beginyearbalance) as opco_currency_opening_bal_amt,
      null as opco_uom_sk,
      null as opco_brand_sk,
      null as opco_sub_ledger_type_sk,
      null as sub_ledger_cd
      from MULE_STAGING.SYSPRO_PRIMEX.COMPANYX_DBO_GENHISTORY as gh
      left join  MULE_STAGING.SYSPRO_PRIMEX.COMPANYX_DBO_CUSGENMASTER_ as cgm
         on gh.glcode=cgm.glcode
      left join __dbt__cte__v_primex_opco_curr opco 
         on opco.opco_id = iff(trim(gh.company) is null, 'X', trim(gh.company))
         and opco.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_chart_of_accts_curr ocoa 
         on ocoa.gl_acct_nbr = trim(gh.glcode)
         and ocoa.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_cost_center_curr occ
         on occ.src_cost_center_cd = upper(trim(cgm.cc)) 
         and occ.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_dept_curr od 
         on od.src_dept_cd = upper(trim(cgm.dept)) 
         and od.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_type_curr ot 
         on ot.src_type_cd = upper(trim(cgm.type)) 
         and ot.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_purpose_curr op 
         on op.src_purpose_cd = upper(trim(cgm.purpose)) 
         and op.src_sys_nm = 'SYSPRO-PRMX'
      left join __dbt__cte__v_primex_opco_lob_curr ol 
         on ol.src_lob_cd = upper(trim(cgm.fsgroup4buname)) 
         and ol.src_sys_nm = 'SYSPRO-PRMX' 
      left join OI_DATA_DEV_V2.FND_DEV_BKP.fscl_clndr fc 
         on fc.fscl_yr_id = trim(gh.glyear)
      where trim(gh.glyear) >= 2020 and trim(gh.company) in (null, 'X')
      
     
),
de_duplication as(
    select *
           , rank() over(partition by opco_fscl_yr_gl_opening_bal_ak order by stg_load_dtm, src_comp) as rnk
    from primex_opco_fscl_yr_gl_opening_bal
),

final as(
    select  distinct 
            opco_fscl_yr_gl_opening_bal_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_chart_of_accts_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_gl_trans_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_gl_trans_posting_type_sk as 
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
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(fscl_yr_strt_dt as 
    varchar
), '') || '-' || coalesce(cast(fscl_yr_nbr as 
    varchar
), '') || '-' || coalesce(cast(credit_ind as 
    varchar
), '') || '-' || coalesce(cast(opening_bal_qty as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_opening_bal_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_opening_bal_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_brand_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sub_ledger_type_sk as 
    varchar
), '') || '-' || coalesce(cast(sub_ledger_cd as 
    varchar
), '') as 
    varchar
)) as OPCO_FSCL_YR_GL_OPENING_BAL_CK,
            opco_fscl_yr_gl_opening_bal_ak,
            CRT_DTM, STG_LOAD_DTM, DELETE_DTM, SRC_SYS_NM, SRC_KEY_TXT, OPCO_SK, OPCO_CHART_OF_ACCTS_SK, OPCO_GL_TRANS_TYPE_SK, OPCO_GL_TRANS_POSTING_TYPE_SK, OPCO_COST_CENTER_SK, OPCO_DEPT_SK, OPCO_TYPE_SK, OPCO_PURPOSE_SK, OPCO_LOB_SK, TRANS_CURRENCY_SK, FSCL_YR_STRT_DT, FSCL_YR_NBR, CREDIT_IND, OPENING_BAL_QTY, TRANS_CURRENCY_OPENING_BAL_AMT, OPCO_CURRENCY_OPENING_BAL_AMT, OPCO_UOM_SK, OPCO_BRAND_SK, OPCO_SUB_LEDGER_TYPE_SK, SUB_LEDGER_CD
    from de_duplication
    where rnk = 1 
),

delete_captured as(
    select * from final
    
        union
        select
        OPCO_FSCL_YR_GL_OPENING_BAL_sk, OPCO_FSCL_YR_GL_OPENING_BAL_ck, OPCO_FSCL_YR_GL_OPENING_BAL_ak, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        SRC_SYS_NM, SRC_KEY_TXT, OPCO_SK, OPCO_CHART_OF_ACCTS_SK, OPCO_GL_TRANS_TYPE_SK, OPCO_GL_TRANS_POSTING_TYPE_SK, OPCO_COST_CENTER_SK, OPCO_DEPT_SK, OPCO_TYPE_SK, OPCO_PURPOSE_SK, OPCO_LOB_SK, TRANS_CURRENCY_SK, FSCL_YR_STRT_DT, FSCL_YR_NBR, CREDIT_IND, OPENING_BAL_QTY, TRANS_CURRENCY_OPENING_BAL_AMT, OPCO_CURRENCY_OPENING_BAL_AMT, OPCO_UOM_SK, OPCO_BRAND_SK, OPCO_SUB_LEDGER_TYPE_SK, SUB_LEDGER_CD     
        from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_FSCL_YR_GL_OPENING_BAL
        
            where OPCO_FSCL_YR_GL_OPENING_BAL_ck not in (select distinct OPCO_FSCL_YR_GL_OPENING_BAL_ck from final)
         
        and src_sys_nm = 'SYSPRO-PRMX'
    
)

select * from delete_captured



    
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_fscl_yr_gl_opening_bal ) and OPCO_FSCL_YR_GL_OPENING_BAL_ck not in (select distinct OPCO_FSCL_YR_GL_OPENING_BAL_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_FSCL_YR_GL_OPENING_BAL where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_fscl_yr_gl_opening_bal ) and OPCO_FSCL_YR_GL_OPENING_BAL_ck in (select distinct OPCO_FSCL_YR_GL_OPENING_BAL_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_FSCL_YR_GL_OPENING_BAL where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    
