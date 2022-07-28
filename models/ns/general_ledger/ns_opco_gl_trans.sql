{{ config(
    post_hook=
    ["UPDATE OI_DATA_DEV.INTERMEDIATE_FND_DEV.NS_OPCO_GL_TRANS
    SET OPCO_COST_CENTER_SK = (SELECT OPCO_COST_CENTER_SK FROM OI_DATA_DEV.INTERMEDIATE_FND_DEV.NS_OPCO_COST_CENTER WHERE SRC_COST_CENTER_CD = '4')
    WHERE SRC_KEY_TXT IN ('28794802|0', '28794802|1', '28794152|0', '28794152|1', '28794477|1', '28794477|0', '29073638|0', '29073638|1', '29072175|0', '29072175|1')" 
    ,
    "UPDATE OI_DATA_DEV.INTERMEDIATE_FND_DEV.NS_OPCO_GL_TRANS
    SET OPCO_COST_CENTER_SK = (SELECT OPCO_COST_CENTER_SK FROM OI_DATA_DEV.INTERMEDIATE_FND_DEV.NS_OPCO_COST_CENTER WHERE SRC_COST_CENTER_CD = '12')
    WHERE SRC_KEY_TXT IN ('261322|0', '261322|12', '261322|13')"]
) }}

{% set _load = load_type('NS_OPCO_GL_TRANS') %}

with ns_opco_gl_trans as (
    select 
    current_timestamp as crt_dtm,
    tl._fivetran_synced as stg_load_dtm,
    case 
        when tl._fivetran_deleted = true then tl._fivetran_synced
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    concat_ws('|', tl.transaction_id, tl.transaction_line_id)::varchar(50) as src_key_txt,
    t.trandate::timestamp as trans_dt,
    opco.opco_sk,
    oca.opco_chart_of_accts_sk,
    ogt.opco_gl_trans_type_sk,
    null::varchar(32) as opco_gl_trans_posting_type_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    oip.opco_purpose_sk,
    null::varchar(32) as opco_lob_sk,
    oc.opco_currency_sk as trans_currency_sk,
    null::varchar(32) as opco_proj_sk,
    null::varchar(32) as original_gl_opco_sk,
    null::varchar(20) as original_gl_voucher_nbr,
    t.document_number::varchar(20) as voucher_nbr,
    t.document_number::varchar(20) as journal_batch_nbr,
    tl.memo::varchar(5000) as gl_desc,
    t.document_number::varchar(20) as document_nbr,
    t.document_date::date as document_dt,
    null::numeric(1,0) as crrctn_trans_ind,
    case 
        when is_leftside = 'F' then 1
        when is_leftside = 'T' then 0
    end::numeric(1,0) as credit_ind,
    tl.opi_qty::float as gl_qty,
    tl.amount_foreign::float as trans_currency_gl_amt,
    tl.amount::float as opco_currency_gl_amt,
    fc.fscl_yr_id as fscl_yr_nbr,
    fc.fscl_mnth_nbr as fscl_mnth_nbr,
    case    
        when a.type_name <> 'Statistical' and  tl.non_posting_line = 'No' then 1
        when a.type_name <> 'Statistical' and  tl.non_posting_line = 'Yes' then 0
        when a.type_name = 'Statistical' then 1
    end::numeric(1,0) as posted_ind,
    ou.opco_uom_sk,
    ob.opco_brand_sk,
    null::varchar(32) as opco_sub_ledger_type_sk,
    null::varchar(32) as sub_ledger_cd
    from {{ source('NS_DEV', 'TRANSACTION_LINES') }} tl
    left join {{ source('NS_DEV', 'TRANSACTIONS')}} t 
        on tl.transaction_id = t.transaction_id
        and t._fivetran_deleted = false
    left join {{ source('NS_DEV', 'ACCOUNTING_PERIODS')}} ap 
        on t.accounting_period_id = ap.accounting_period_id
        and ap._fivetran_deleted = false
    left join {{ source('NS_DEV', 'ACCOUNTS')}} a
        on tl.account_id = a.account_id
        and a._fivetran_deleted = false
    left join {{ ref('opco')}} opco 
        on tl.subsidiary_id = opco.opco_id
        and opco.src_sys_nm = 'NS'
    left join {{ ref('opco_chart_of_accts')}} oca 
        on a.accountnumber = oca.gl_acct_nbr
        and tl.subsidiary_id = oca.opco_id
        and oca.src_sys_nm = 'NS'
    left join {{ ref('opco_gl_trans_type')}} ogt 
        on upper(t.transaction_type) = ogt.src_gl_trans_type_cd
        and ogt.src_sys_nm = 'NS'
    left join {{ ref('opco_cost_center')}} occ 
        on upper(tl.location_id) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'NS'
    left join {{ ref('opco_dept')}} od 
        on upper(tl.department_id) = od.src_dept_cd
        and od.src_sys_nm = 'NS'
    left join {{ ref('opco_type')}} ot 
        on upper(substr(a.opi_ax_acct, 8)) = ot.src_type_cd
        and ot.src_sys_nm = 'NS'
    left join {{ ref('opco_purpose')}} oip
        on upper(tl.market_opi_id) = oip.src_purpose_cd
        and oip.src_sys_nm = 'NS'
    left join {{ ref('opco_lob')}} ol
        on upper(tl.class_id) = ol.src_lob_cd
        and ol.src_sys_nm = 'NS'
    left join {{ ref('opco_currency')}} oc
        on upper(t.currency_id) = oc.src_currency_cd
        and oc.src_sys_nm = 'NS'
    left join {{ ref('opco_uom')}} ou 
        on ou.src_uom_cd = upper(a.opi_qty_uom_id)
        and ou.src_sys_nm = 'NS'
    left join {{ ref('opco_brand')}} ob 
        on tl.class_id = ob.src_brand_cd
        and ob.src_sys_nm = 'NS'
    left join {{ ref('fscl_clndr')}} fc 
        on fc.clndr_dt = date(ap.ending)
    where tl.account_id is not null
),
final as (
    select  distinct
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_key_txt']) }} as opco_gl_trans_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_key_txt', 'trans_dt', 'opco_sk', 'opco_chart_of_accts_sk', 'opco_gl_trans_type_sk', 'opco_gl_trans_posting_type_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'trans_currency_sk', 'opco_proj_sk', 'original_gl_opco_sk', 'original_gl_voucher_nbr', 'voucher_nbr', 'journal_batch_nbr', 'gl_desc', 'document_nbr', 'document_dt', 'crrctn_trans_ind', 'gl_qty', 'trans_currency_gl_amt', 'opco_currency_gl_amt', 'fscl_yr_nbr', 'fscl_mnth_nbr', 'posted_ind', 'opco_uom_sk', 'opco_brand_sk', 'opco_sub_ledger_type_sk', 'sub_ledger_cd']) }} as opco_gl_trans_ck,
            concat_ws('|', src_sys_nm, src_key_txt)::varchar(100) as opco_gl_trans_ak,
            * 
    from ns_opco_gl_trans ogt
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'NS_OPCO_GL_TRANS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_GL_TRANS') and _load != 3%}
    union
    select
    OPCO_GL_TRANS_SK, OPCO_GL_TRANS_CK, OPCO_GL_TRANS_AK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, src_key_txt, trans_dt, opco_sk, opco_chart_of_accts_sk, opco_gl_trans_type_sk, opco_gl_trans_posting_type_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, trans_currency_sk, opco_proj_sk, original_gl_opco_sk, original_gl_voucher_nbr, voucher_nbr, journal_batch_nbr, gl_desc, document_nbr, document_dt, crrctn_trans_ind, credit_ind, gl_qty, trans_currency_gl_amt, opco_currency_gl_amt, fscl_yr_nbr, fscl_mnth_nbr, posted_ind, opco_uom_sk, opco_brand_sk, opco_sub_ledger_type_sk, sub_ledger_cd  
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS
    {% if _load == 1 %}
        where OPCO_GL_TRANS_ck not in (select distinct OPCO_GL_TRANS_ck from final)
    {% else %}
        where OPCO_GL_TRANS_ck not in (select distinct OPCO_GL_TRANS_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS where src_sys_nm = 'NS')
    {% endif %} 
    and src_sys_nm = 'NS'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_GL_TRANS_ck not in (select distinct OPCO_GL_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS where src_sys_nm = 'NS')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_GL_TRANS_ck in (select distinct OPCO_GL_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS where src_sys_nm = 'NS') and delete_dtm is not null)
    {% endif %}
{% endif %}
    