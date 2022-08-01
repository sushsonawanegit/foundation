

with  __dbt__cte__v_jde_opco_curr as (
with v_jde_opco as(
    select *,
    rank() over(partition by opco_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco
),
final as(
    select 
    *
    from v_jde_opco
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_jde_opco_chart_of_accts_curr as (
with v_jde_opco_chart_of_accts as(
    select *,
    rank() over(partition by opco_chart_of_accts_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_chart_of_accts
),
final as(
    select 
    *
    from v_jde_opco_chart_of_accts
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_jde_opco_gl_trans_type_curr as (
with v_jde_opco_gl_trans_type as(
    select *,
    rank() over(partition by opco_gl_trans_type_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_gl_trans_type
),
final as(
    select 
    *
    from v_jde_opco_gl_trans_type
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_jde_opco_cost_center_curr as (
with v_jde_opco_cost_center as(
    select *,
    rank() over(partition by opco_cost_center_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_cost_center
),
final as(
    select 
    *
    from v_jde_opco_cost_center
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_jde_opco_currency_curr as (
with v_jde_opco_currency as(
    select *,
    rank() over(partition by opco_currency_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_currency
),
final as(
    select 
    *
    from v_jde_opco_currency
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_jde_opco_uom_curr as (
with v_jde_opco_uom as(
    select *,
    rank() over(partition by opco_uom_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_uom
),
final as(
    select 
    *
    from v_jde_opco_uom
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_jde_opco_sub_ledger_type_curr as (
with v_jde_opco_sub_ledger_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_sub_ledger_type
)

select * from v_jde_opco_sub_ledger_type
),grouped_data as(
    select 
    'JDE-HNCK' as src_sys_nm,
    upper(trim(f0911.glco)) as glco,
    upper(trim(f0911.gldoc)) as gldoc,
    upper(trim(f0911.gldct)) as gldct,
    upper(trim(f0911.gljeln)) as gljeln,
    upper(trim(f0911.glextl)) as glextl,
    upper(trim(f0911.glani)) as glani,
    upper(trim(f0911.gldgj)) as gldgj,
    max(trim(f0911.gldicj)) as gldicj,
    max(trim(f0911.glicu)) as glicu,
    max(trim(f0911.glmcu)) as glmcu,
    max(trim(f0911.glcrcd)) as glcrcd,
    max(trim(f0911.glodoc)) as glodoc,
    max(trim(f0911.glexa)) as glexa,
    max(trim(f0911.glpost)) as glpost,
    max(trim(f0911.glum)) as glum,
    max(trim(f0911.glsblt)) as glsblt,
    max(trim(f0911.glsbl)) as glsbl,
    max(trim(f0911.glctry))*100 + max(trim(f0911.glfy)) as fscl_yr_nbr,
    max(trim(f0911.glpn)) as fscl_mnth_nbr,
    max(trim(f0911.glkco)) as sub_document_nbr,
    sum(trim(f0911.glu))/(select pow(10, frcdec::number) from MULE_STAGING.JDE_HANCOCK.JDE920_DD920_F9210 where trim(frdtai) = 'U') as gl_qty,
    sum(iff(trim(f0911.glacr) = 0 or trim(f0911.glacr) = trim(f0911.glaa), trim(f0911.glaa), trim(f0911.glacr)))/(select pow(10, frcdec::number) from MULE_STAGING.JDE_HANCOCK.JDE920_DD920_F9210 where trim(frdtai) = 'AA') as trans_currency_gl_amt,
    sum(trim(f0911.glaa))/(select pow(10, frcdec::number) from MULE_STAGING.JDE_HANCOCK.JDE920_DD920_F9210 where trim(frdtai) = 'AA') as opco_currency_gl_amt
    from MULE_STAGING.JDE_HANCOCK.JDE_PRODUCTION_PRODDTA_F0911  f0911 
    where trim(f0911.gllt) = 'AA' and trim(f0911.glco) = '03000'
    and trim(f0911.glani) not in ('30.6001', '30.6002', '30.6003', '30.6004', '30.6005', '30.6006', '30.6020', '30.6021', '30.6022', '30.6023', '30.6026', '30.6028', '30.6030', '30.6032', '30.6034', '31.6001', '31.6002', '31.6004', '31.6005', '31.6006', '31.6020', '31.6021', '31.6022', '31.6023', '31.6026', '31.6028', '31.6030', '31.6032', '31.6034', '32.6001', '32.6002', '32.6004', '32.6005', '32.6020', '32.6021', '32.6022', '32.6023', '32.6026', '32.6028', '32.6030', '32.6032', '32.6034', '33.6001', '33.6002', '33.6004', '33.6004.1', '33.6005', '33.6006', '33.6020', '33.6021', '33.6022', '33.6023', '33.6026', '33.6028', '33.6030', '33.6032', '33.6034', '301.2102', '301.5201', '301.5210', '301.5210.3', '301.5290', '301.6001', '301.6002', '301.6004', '301.6004.1', '301.6005', '301.6006', '301.6007', '301.6020', '301.6021', '301.6022', '301.6023', '301.6026', '301.6028', '301.6030', '301.6032', '301.6034', '302.2102', '302.5201', '302.5210', '302.5210.3', '302.5290', '302.6001', '302.6002', '302.6004', '302.6004.1', '302.6005', '302.6006', '302.6007', '302.6020', '302.6021', '302.6022', '302.6023', '302.6026', '302.6028', '302.6030', '302.6032', '302.6034', '303.2102', '303.5201', '303.5210', '303.5210.3', '303.5290', '303.6001', '303.6002', '303.6004', '303.6004.1', '303.6005', '303.6006', '303.6007', '303.6020', '303.6021', '303.6022', '303.6023', '303.6026', '303.6028', '303.6030', '303.6032', '303.6034', '304.2102', '304.5201', '304.5210', '304.5210.3', '304.5290', '304.6001', '304.6002', '304.6004', '304.6004.1', '304.6005', '304.6006', '304.6007', '304.6020', '304.6021', '304.6022', '304.6023', '304.6026', '304.6028', '304.6030', '304.6032', '304.6034', '306.2102', '306.5201', '306.5210', '306.5210.3', '306.5290', '306.6001', '306.6002', '306.6004', '306.6004.1', '306.6005', '306.6006', '306.6007', '306.6020', '306.6021', '306.6022', '306.6023', '306.6026', '306.6028', '306.6030', '306.6032', '306.6034', '307.2102', '307.5201', '307.5210', '307.5210.3', '307.5290', '307.6001', '307.6002', '307.6004', '307.6004.1', '307.6005', '307.6006', '307.6007', '307.6020', '307.6021', '307.6022', '307.6023', '307.6026', '307.6028', '307.6030', '307.6032', '307.6034', '312.6001', '312.6002', '312.6004', '312.6004.1', '312.6005', '312.6006', '312.6020', '312.6021', '312.6022', '312.6023', '312.6026', '312.6028', '312.6030', '312.6032', '312.6034', '322.6001', '322.6002', '322.6004', '322.6005', '322.6006', '322.6020', '322.6021', '322.6022', '322.6023', '322.6026', '322.6028', '322.6030', '322.6032', '322.6034', '332.6001', '332.6002', '332.6004', '332.6005', '332.6006', '332.6020', '332.6021', '332.6022', '332.6023', '332.6026', '332.6028', '332.6030', '332.6032', '332.6034', '342.6001', '342.6002', '342.6004', '342.6005', '342.6006', '342.6020', '342.6021', '342.6022', '342.6023', '342.6026', '342.6028', '342.6030', '342.6032', '342.6034', '362.6001', '362.6002', '362.6004', '362.6005', '362.6006', '362.6020', '362.6021', '362.6022', '362.6023', '362.6026', '362.6028', '362.6030', '362.6032', '362.6034', '372.6001', '372.6002', '372.6004', '372.6005', '372.6006', '372.6020', '372.6021', '372.6022', '372.6023', '372.6026', '372.6028', '372.6030', '372.6032', '372.6034', '3000.2102', '3000.2103', '3000.6001', '3000.6002', '3000.6004', '3000.6004.1', '3000.6005', '3000.6006', '3000.6007', '3000.6020', '3000.6021', '3000.6022', '3000.6023', '3000.6026', '3000.6028', '3000.6030', '3000.6032', '3000.6034', '3000.6161')
    group by glco, gldoc, gldct, gljeln, glextl, glani, gldgj
),
opco_gl_trans as (
    select 
    md5(cast(coalesce(cast(gd.src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(glco as 
    varchar
), '') || '-' || coalesce(cast(gldoc as 
    varchar
), '') || '-' || coalesce(cast(gldct as 
    varchar
), '') || '-' || coalesce(cast(gljeln as 
    varchar
), '') || '-' || coalesce(cast(glextl as 
    varchar
), '') || '-' || coalesce(cast(glani as 
    varchar
), '') || '-' || coalesce(cast(gldgj as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_sk,
    concat_ws('|', gd.src_sys_nm, glco, gldoc, gldct, gljeln, glextl, glani, gldgj) as opco_gl_trans_ak,
    current_timestamp as crt_dtm,
    null::timestamp_tz as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    gd.src_sys_nm, 
    null as src_key_txt,
    case 
        when trim(gd.gldgj) = 0 then iff(length(gd.gldicj) = 6, 
    
        date_from_parts(
    concat((substring(gd.gldicj, 1, 1)::int + 19)::varchar, substring(gd.gldicj, 2, 2))
, 1, 
    substring(gd.gldicj, 4, 3)
)
    
, null)
        else iff(length(gd.gldgj) = 6, 
    
        date_from_parts(
    concat((substring(gd.gldgj, 1, 1)::int + 19)::varchar, substring(gd.gldgj, 2, 2))
, 1, 
    substring(gd.gldgj, 4, 3)
)
    
, null)
    end as trans_dt,
    opco.opco_sk,
    ocoa.opco_chart_of_accts_sk,
    ogtt.opco_gl_trans_type_sk, 
    null as opco_gl_trans_posting_type_sk,
    occ.opco_cost_center_sk,
    null as opco_dept_sk,
    null as opco_type_sk,
    null as opco_purpose_sk,
    null as opco_lob_sk,
    oc.opco_currency_sk as trans_currency_sk,
    null as opco_proj_sk,
    null as original_gl_opco_sk,
    gd.glodoc as original_gl_voucher_nbr,
    gd.gldoc as voucher_nbr,
    gd.glicu as journal_batch_nbr,
    gd.glexa as gl_desc,
    gd.gldoc as document_nbr,
    case 
        when trim(gd.gldgj) = 0 then iff(length(gd.gldicj) = 6, 
    
        date_from_parts(
    concat((substring(gd.gldicj, 1, 1)::int + 19)::varchar, substring(gd.gldicj, 2, 2))
, 1, 
    substring(gd.gldicj, 4, 3)
)
    
, null)
        else iff(length(gd.gldgj) = 6, 
    
        date_from_parts(
    concat((substring(gd.gldgj, 1, 1)::int + 19)::varchar, substring(gd.gldgj, 2, 2))
, 1, 
    substring(gd.gldgj, 4, 3)
)
    
, null)
    end as document_dt,
    null as crrctn_trans_ind,
    null as credit_ind,
    gd.gl_qty,
    gd.trans_currency_gl_amt,
    gd.opco_currency_gl_amt,
    gd.fscl_yr_nbr,
    gd.fscl_mnth_nbr,
    iff(gd.glpost = 'P', 1, 0) as posted_ind,
    ou.opco_uom_sk,
    null as opco_brand_sk,
    oslt.opco_sub_ledger_type_sk,
    gd.glsbl as sub_ledger_cd,
    gd.sub_document_nbr
    from grouped_data gd 
    left join __dbt__cte__v_jde_opco_curr opco 
        on gd.glco = opco.opco_id 
        and opco.src_sys_nm = 'JDE-HNCK'
    left join __dbt__cte__v_jde_opco_chart_of_accts_curr ocoa 
        on gd.glani = ocoa.gl_acct_nbr
        and ocoa.src_sys_nm = 'JDE-HNCK'    
    left join __dbt__cte__v_jde_opco_gl_trans_type_curr ogtt 
        on gd.gldct = ogtt.src_gl_trans_type_cd
        and ogtt.src_sys_nm = 'JDE-HNCK'  
    left join __dbt__cte__v_jde_opco_cost_center_curr occ 
        on upper(gd.glmcu) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'JDE-HNCK'  
    left join __dbt__cte__v_jde_opco_currency_curr oc    
        on upper(gd.glcrcd) = oc.src_currency_cd
        and oc.src_sys_nm = 'JDE_HNCK'
    left join __dbt__cte__v_jde_opco_uom_curr ou 
        on upper(gd.glum) = ou.src_uom_cd
        and ou.src_sys_nm = 'JDE-HNCK'
    left join __dbt__cte__v_jde_opco_sub_ledger_type_curr oslt 
        on upper(gd.glsblt) = oslt.src_sub_ledger_type_cd
        and oslt.src_sys_nm = 'JDE-HNCK'
),
final as (
    select distinct 
        OPCO_GL_TRANS_SK,  
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') || '-' || coalesce(cast(trans_dt as 
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
), '') || '-' || coalesce(cast(opco_proj_sk as 
    varchar
), '') || '-' || coalesce(cast(original_gl_opco_sk as 
    varchar
), '') || '-' || coalesce(cast(original_gl_voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(journal_batch_nbr as 
    varchar
), '') || '-' || coalesce(cast(gl_desc as 
    varchar
), '') || '-' || coalesce(cast(document_nbr as 
    varchar
), '') || '-' || coalesce(cast(document_dt as 
    varchar
), '') || '-' || coalesce(cast(crrctn_trans_ind as 
    varchar
), '') || '-' || coalesce(cast(credit_ind as 
    varchar
), '') || '-' || coalesce(cast(gl_qty as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_gl_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_gl_amt as 
    varchar
), '') || '-' || coalesce(cast(fscl_yr_nbr as 
    varchar
), '') || '-' || coalesce(cast(fscl_mnth_nbr as 
    varchar
), '') || '-' || coalesce(cast(posted_ind as 
    varchar
), '') || '-' || coalesce(cast(opco_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_brand_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sub_ledger_type_sk as 
    varchar
), '') || '-' || coalesce(cast(sub_ledger_cd as 
    varchar
), '') || '-' || coalesce(cast(sub_document_nbr as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_ck,
        OPCO_GL_TRANS_AK, CRT_DTM, STG_LOAD_DTM, DELETE_DTM, SRC_SYS_NM, SRC_KEY_TXT, TRANS_DT, OPCO_SK, OPCO_CHART_OF_ACCTS_SK, OPCO_GL_TRANS_TYPE_SK, OPCO_GL_TRANS_POSTING_TYPE_SK, OPCO_COST_CENTER_SK, OPCO_DEPT_SK, OPCO_TYPE_SK, OPCO_PURPOSE_SK, OPCO_LOB_SK, TRANS_CURRENCY_SK, OPCO_PROJ_SK, ORIGINAL_GL_OPCO_SK, ORIGINAL_GL_VOUCHER_NBR, VOUCHER_NBR, JOURNAL_BATCH_NBR, GL_DESC, DOCUMENT_NBR, DOCUMENT_DT, CRRCTN_TRANS_IND, CREDIT_IND, GL_QTY, TRANS_CURRENCY_GL_AMT, OPCO_CURRENCY_GL_AMT, fscl_yr_nbr, fscl_mnth_nbr, POSTED_IND, OPCO_UOM_SK, OPCO_BRAND_SK, opco_sub_ledger_type_sk, sub_ledger_cd, sub_document_nbr
    from opco_gl_trans
),
delete_captured as(
    select * from final
    
        union
        select
        OPCO_GL_TRANS_sk, OPCO_GL_TRANS_ck, OPCO_GL_TRANS_AK, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        src_sys_nm, src_key_txt, trans_dt, opco_sk, opco_chart_of_accts_sk, opco_gl_trans_type_sk, opco_gl_trans_posting_type_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, trans_currency_sk, opco_proj_sk, original_gl_opco_sk, original_gl_voucher_nbr, voucher_nbr, journal_batch_nbr, gl_desc, document_nbr, document_dt, crrctn_trans_ind, credit_ind, gl_qty, trans_currency_gl_amt, opco_currency_gl_amt, fscl_yr_nbr, fscl_mnth_nbr, posted_ind, opco_uom_sk, opco_brand_sk, opco_sub_ledger_type_sk, sub_ledger_cd, sub_document_nbr
        from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_GL_TRANS
        
            where OPCO_GL_TRANS_ck not in (select distinct OPCO_GL_TRANS_ck from final)
         
        and src_sys_nm = 'JDE-HNCK'
    
)

select * from delete_captured



    
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_gl_trans ) and OPCO_GL_TRANS_ck not in (select distinct OPCO_GL_TRANS_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_GL_TRANS where src_sys_nm = 'JDE-HNCK')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_gl_trans ) and OPCO_GL_TRANS_ck in (select distinct OPCO_GL_TRANS_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_GL_TRANS where src_sys_nm = 'JDE-HNCK') and delete_dtm is not null)
    
