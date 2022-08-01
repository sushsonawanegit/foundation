{% set _load = load_type('AX_OPCO_GL_TRANS') %}

with ax_opco_gl_trans as (
    select 
    current_timestamp as crt_dtm,
    lt._fivetran_synced as stg_load_dtm,
    case 
        when lt._fivetran_deleted = true then lt._fivetran_synced
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    lt.recid::varchar(50) as src_key_txt,
    lt.transdate as trans_dt,
    opco.opco_sk,
    oca.opco_chart_of_accts_sk,
    ogt.opco_gl_trans_type_sk,
    ogtp.opco_gl_trans_posting_type_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    oip.opco_purpose_sk,
    ol.opco_lob_sk,
    oc.opco_currency_sk as trans_currency_sk,
    {{ dbt_utils.surrogate_key(["'AX'", 'lt.dataareaid', 'lt.projid_opi']) }} as opco_proj_sk,
    {{ dbt_utils.surrogate_key(["'AX'", 'lt.origincompany_opi']) }} as original_gl_opco_sk,
    lt.originvoucher_opi::varchar(20) as original_gl_voucher_nbr,
    lt.voucher::varchar(20) as voucher_nbr,
    lt.journalnum::varchar(20) as journal_batch_nbr,
    lt.txt::varchar(5000) as gl_desc,
    lt.documentnum::varchar(20) as document_nbr,
    lt.documentdate::date as document_dt,
    lt.correct::numeric(1,0) as crrctn_trans_ind,
    lt.crediting::numeric(1,0) as credit_ind,
    case 
        when ltb.accountpltype = 100 and ltb.unitid_opi in ('$', null, ' ', '') then null
        when ltb.accountpltype = 100 and ltb.unitid_opi not in ('$', null, ' ', '') then lt.qty
        else lt.qty 
    end::float as gl_qty,
    lt.amountcur::float as trans_currency_gl_amt,
    case 
        when ltb.accountpltype = 100 and ltb.unitid_opi in ('$', null, ' ', '') then lt.qty
        when ltb.accountpltype = 100 and ltb.unitid_opi not in ('$', null, ' ', '') then null
        else lt.amountmst
    end::float as opco_currency_gl_amt,
    fc.fscl_yr_id as fscl_yr_nbr,
    fc.fscl_mnth_nbr as fscl_mnth_nbr,
    1::numeric(1,0) as posted_ind,
    ou.opco_uom_sk,
    null::varchar(32) as opco_brand_sk,
    null::varchar(32) as opco_sub_ledger_type_sk,
    null::varchar(32) as sub_ledger_cd
    from {{ source('AX_DEV', 'LEDGERTRANS') }} lt
    left join {{ source('AX_DEV', 'LEDGERTABLE')}} ltb 
        on lt.dataareaid = ltb.dataareaid
        and lt.accountnum = ltb.accountnum
        and ltb._fivetran_deleted = false
        and ltb.dataareaid not in {{ var('excluded_ax_companies')}}
    left join {{ ref('ax_opco_curr')}} opco 
        on lt.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_chart_of_accts_curr')}} oca 
        on lt.dataareaid = oca.opco_id
        and lt.accountnum = oca.gl_acct_nbr
        and oca.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_gl_trans_type_curr')}} ogt 
        on lt.transtype::string = ogt.src_gl_trans_type_cd
        and ogt.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_gl_trans_posting_type_curr')}} ogtp 
        on lt.posting = ogtp.src_gl_trans_posting_type_cd
        and ogtp.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_cost_center_curr')}} occ 
        on upper(lt.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_dept_curr')}} od 
        on upper(lt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_type_curr')}} ot 
        on upper(lt.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_purpose_curr')}} oip
        on upper(lt.dimension4_) = oip.src_purpose_cd
        and oip.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_lob_curr')}} ol
        on upper(lt.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_currency_curr')}} oc
        on upper(lt.currencycode) = oc.src_currency_cd
        and oc.src_sys_nm = 'AX'
    left join {{ ref('ax_fscl_clndr_curr')}} fc
        on fc.clndr_dt = date(lt.transdate)
    left join {{ ref('ax_opco_uom_curr')}} ou 
        on ou.src_uom_cd = upper(ltb.unitid_opi)
        and ou.src_sys_nm = 'AX'
    where lt.periodcode = 1
    and lt.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select  distinct
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_key_txt']) }} as opco_gl_trans_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_key_txt', 'trans_dt', 'opco_sk', 'opco_chart_of_accts_sk', 'opco_gl_trans_type_sk', 'opco_gl_trans_posting_type_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'trans_currency_sk', 'opco_proj_sk', 'original_gl_opco_sk', 'original_gl_voucher_nbr', 'voucher_nbr', 'journal_batch_nbr', 'gl_desc', 'document_nbr', 'document_dt', 'crrctn_trans_ind', 'credit_ind', 'gl_qty', 'trans_currency_gl_amt', 'opco_currency_gl_amt', 'fscl_yr_nbr', 'fscl_mnth_nbr', 'posted_ind', 'opco_uom_sk', 'opco_brand_sk', 'opco_sub_ledger_type_sk', 'sub_ledger_cd']) }} as opco_gl_trans_ck,
            concat_ws('|', src_sys_nm, src_key_txt)::varchar(100) as opco_gl_trans_ak,
            * 
    from ax_opco_gl_trans 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_GL_TRANS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_GL_TRANS') and _load != 3%}
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
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_GL_TRANS_ck not in (select distinct OPCO_GL_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_GL_TRANS_ck in (select distinct OPCO_GL_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}