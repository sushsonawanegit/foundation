{{
    config(
        materialized='incremental',
        transient=false,
        unique_key='opco_gl_trans_sk'
    )
}}

{% set incrmnt_col=increment_col('OPCO_GL_TRANS') %}

with opco_gl_trans as(
    select *,
    rank() over(partition by opco_gl_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_gl_trans_lineage') }}
),
final as(
    select 
    opco_gl_trans_SK, opco_gl_trans_CK, opco_gl_trans_ak, CRT_DTM, STG_LOAD_DTM, DELETE_DTM, src_sys_nm, src_key_txt, trans_dt, opco_sk, opco_chart_of_accts_sk, opco_gl_trans_type_sk, opco_gl_trans_posting_type_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, trans_currency_sk, opco_proj_sk, original_gl_opco_sk, original_gl_voucher_nbr, voucher_nbr, journal_batch_nbr, gl_desc, document_nbr, document_dt, crrctn_trans_ind, credit_ind, gl_qty, trans_currency_gl_amt, opco_currency_gl_amt, fscl_yr_nbr, fscl_mnth_nbr, posted_ind, opco_uom_sk, opco_brand_sk, opco_sub_ledger_type_sk, sub_ledger_cd, sub_document_nbr  
    from opco_gl_trans
    where rnk = 1 and delete_dtm is null
)

select * from final

{% if is_incremental() %}
    where {{ incrmnt_col}} >= (select min({{ incrmnt_col}}) from {{ var('fnd_tbl_db')}}.{{ var('intermediate_tbl_sch')}}.V_OPCO_GL_TRANS_LINEAGE where stg_load_dtm >= 
                                (select min(max_stg_load_dtm) from {{ var('fnd_tbl_db')}}.{{ var('intermediate_tbl_sch')}}._FOUNDATION_LOAD_CONFIG where target_model_nm = 'OPCO_GL_TRANS'))
{% endif %}