{% set _load = load_type('JDE_NPP_OPCO_GL_TRANS') %}

with grouped_data as(
    select
    'JDE-NPP' as src_sys_nm,
    trim(glco) as glco, 
    trim(gldoc) as gldoc, 
    trim(glkco) as glkco, 
    trim(gldct) as gldct, 
    trim(gljeln) as gljeln, 
    trim(glextl) as glextl, 
    trim(glani) as glani, 
    trim(gldgj) as gldgj,
    max(mule_load_ts) as stg_load_dtm,
    max(trim(glmcu)) as glmcu,
    max(trim(glcrcd)) as glcrcd,
    max(trim(glodoc)) as glodoc,
    max(trim(glicu)) as glicu,
    max(trim(glexa)) as glexa,
    sum(trim(glu))/100 as gl_qty,
    sum(trim(glaa))/100 as trans_currency_gl_amt,
    sum(trim(glaa))/100 as opco_currency_gl_amt,
    max(trim(glctry))*100 + max(trim(glfy)) as fscl_yr_nbr,
    max(trim(glpn)) as fscl_mnth_nbr,
    max(trim(glpost)) as glpost,
    max(trim(glum)) as glum,
    max(trim(glsblt)) as glsblt,
    max(trim(glsbl)) as glsbl
    from {{source('JDE_NPP_DEV1','F0911')}}
    where trim(gllt) = 'AA' and trim(glco) in ('00001', '00006', '00009') and trim(gldgj) >= '121273' and trim(gldgj) != 0 and trim(gldgj) is not null
    group by glco, gldoc, glkco, gldct, gljeln, glextl, glani, gldgj
),
jde_npp_opco_gl_trans as(
    select 
    {{ dbt_utils.surrogate_key(['gd.src_sys_nm', 'gd.glco', 'gd.gldoc', 'gd.glkco', 'gd.gldct', 'gd.gljeln', 'gd.glextl', 'gd.glani', 'gd.gldgj'])}} as opco_gl_trans_sk,
    concat_ws('|', gd.src_sys_nm, gd.glco, gd.gldoc, gd.glkco, gd.gldct, gd.gljeln, gd.glextl, gd.glani, gd.gldgj) as opco_gl_trans_ak,
    current_timestamp as crt_dtm,
    gd.stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    gd.src_sys_nm, 
    concat_ws('|', upper(trim(gd.glco)), upper(trim(gd.gldoc)), upper(trim(gd.glkco)), upper(trim(gd.gldct)), upper(trim(gd.gljeln)), upper(trim(gd.glextl)), upper(trim(gd.glani)), upper(trim(gd.gldgj))) as src_key_txt,
    iff(length(gd.gldgj) = 6, {{ julian_to_clndr_dt('gd.gldgj')}}, null) as trans_dt,
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
    iff(length(gd.gldgj) = 6, {{ julian_to_clndr_dt('gd.gldgj')}}, null) as document_dt,
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
    gd.glkco as sub_document_nbr
    from grouped_data gd
    left join {{ ref('jde_npp_opco_curr') }} opco 
        on gd.glco = opco.opco_id 
        and opco.src_sys_nm = 'JDE-NPP'
    left join {{ ref('jde_npp_opco_chart_of_accts_curr') }} ocoa 
        on gd.glani = ocoa.gl_acct_nbr
        and ocoa.src_sys_nm = 'JDE-NPP'    
    left join {{ ref('jde_npp_opco_gl_trans_type_curr')}} ogtt 
        on gd.gldct = ogtt.src_gl_trans_type_cd
        and ogtt.src_sys_nm = 'JDE-NPP'  
    left join {{ ref('jde_npp_opco_cost_center_curr')}} occ 
        on upper(gd.glmcu) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'JDE-NPP'  
    left join {{ ref('jde_npp_opco_currency_curr')}} oc    
        on upper(gd.glcrcd) = oc.src_currency_cd
        and oc.src_sys_nm = 'JDE-NPP'
    left join {{ ref('jde_npp_opco_uom_curr')}} ou 
        on upper(gd.glum) = ou.src_uom_cd
        and ou.src_sys_nm = 'JDE-NPP'
    left join {{ ref('jde_npp_opco_sub_ledger_type_curr')}} oslt 
        on upper(gd.glsblt) = oslt.src_sub_ledger_type_cd
        and oslt.src_sys_nm = 'JDE-NPP' 
),
final as (
    select distinct 
        OPCO_GL_TRANS_SK,  
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_key_txt', 'trans_dt', 'opco_sk', 'opco_chart_of_accts_sk', 'opco_gl_trans_type_sk', 'opco_gl_trans_posting_type_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'trans_currency_sk', 'opco_proj_sk', 'original_gl_opco_sk', 'original_gl_voucher_nbr', 'voucher_nbr', 'journal_batch_nbr', 'gl_desc', 'document_nbr', 'document_dt', 'crrctn_trans_ind', 'credit_ind', 'gl_qty', 'trans_currency_gl_amt', 'opco_currency_gl_amt', 'fscl_yr_nbr', 'fscl_mnth_nbr', 'posted_ind', 'opco_uom_sk', 'opco_brand_sk', 'opco_sub_ledger_type_sk', 'sub_ledger_cd', 'sub_document_nbr']) }} as opco_gl_trans_ck,
        OPCO_GL_TRANS_AK, CRT_DTM, STG_LOAD_DTM, DELETE_DTM, SRC_SYS_NM, SRC_KEY_TXT, TRANS_DT, OPCO_SK, OPCO_CHART_OF_ACCTS_SK, OPCO_GL_TRANS_TYPE_SK, OPCO_GL_TRANS_POSTING_TYPE_SK, OPCO_COST_CENTER_SK, OPCO_DEPT_SK, OPCO_TYPE_SK, OPCO_PURPOSE_SK, OPCO_LOB_SK, TRANS_CURRENCY_SK, OPCO_PROJ_SK, ORIGINAL_GL_OPCO_SK, ORIGINAL_GL_VOUCHER_NBR, VOUCHER_NBR, JOURNAL_BATCH_NBR, GL_DESC, DOCUMENT_NBR, DOCUMENT_DT, CRRCTN_TRANS_IND, CREDIT_IND, GL_QTY, TRANS_CURRENCY_GL_AMT, OPCO_CURRENCY_GL_AMT, fscl_yr_nbr, fscl_mnth_nbr, POSTED_IND, OPCO_UOM_SK, OPCO_BRAND_SK, opco_sub_ledger_type_sk, sub_ledger_cd, sub_document_nbr
    from jde_npp_opco_gl_trans
),
delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'JDE_NPP_OPCO_GL_TRANS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_GL_TRANS') and _load != 3%}
        union
        select
        OPCO_GL_TRANS_sk, OPCO_GL_TRANS_ck, OPCO_GL_TRANS_AK, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        src_sys_nm, src_key_txt, trans_dt, opco_sk, opco_chart_of_accts_sk, opco_gl_trans_type_sk, opco_gl_trans_posting_type_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, trans_currency_sk, opco_proj_sk, original_gl_opco_sk, original_gl_voucher_nbr, voucher_nbr, journal_batch_nbr, gl_desc, document_nbr, document_dt, crrctn_trans_ind, credit_ind, gl_qty, trans_currency_gl_amt, opco_currency_gl_amt, fscl_yr_nbr, fscl_mnth_nbr, posted_ind, opco_uom_sk, opco_brand_sk, opco_sub_ledger_type_sk, sub_ledger_cd, sub_document_nbr
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS
        {% if _load == 1 %}
            where OPCO_GL_TRANS_ck not in (select distinct OPCO_GL_TRANS_ck from final)
        {% else %}
            where OPCO_GL_TRANS_ck not in (select distinct OPCO_GL_TRANS_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS where src_sys_nm = 'JDE-NPP')
        {% endif %} 
        and src_sys_nm = 'JDE-NPP'
    {% endif %}
)

select * from delete_captured


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_GL_TRANS_ck not in (select distinct OPCO_GL_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS where src_sys_nm = 'JDE-NPP')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_GL_TRANS_ck in (select distinct OPCO_GL_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS where src_sys_nm = 'JDE-NPP') and delete_dtm is not null)
    {% endif %}
{% endif %}     