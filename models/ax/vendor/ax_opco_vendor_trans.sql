{% set _load = load_type('AX_OPCO_VENDOR_TRANS') %}

with ax_opco_vendor_trans as(
    select
    current_timestamp as crt_dtm,
    vt._fivetran_synced as stg_load_dtm,
    case
        when vt._fivetran_deleted = true then vt._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    vt.dataareaid as opco_id,
    vt.recid as src_key_txt,
    opco.opco_sk,
    ov.opco_vendor_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    opo.opco_po_sk,
    ogt.opco_gl_trans_type_sk,
    oc.opco_currency_sk as trans_currency_sk,
    opm.opco_pymnt_mode_sk,
    ocd.opco_cash_dscnt_terms_sk,
    opco1.opco_sk as ltst_settled_opco_sk,
    ov1.opco_vendor_sk as ltst_settled_vendor_sk,
    vt.txt as trans_desc,
    vt.transdate as trans_dt,
    vt.duedate as due_dt,
    vt.closed as trans_close_dt,
    vt.voucher as voucher_nbr,
    vt.invoice as invoice_nbr,
    vt.lastsettlevoucher as ltst_settled_voucher_nbr,
    vt.lastsettledate as ltst_settle_dt,
    vt.approved as trans_apprvd_ind,
    vt.approvedby as trans_apprvd_by_txt,
    vt.approveddate as trans_apprvd_dtm,
    vt.documentnum as document_nbr,
    vt.documentdate as document_dt,
    vt.offsetrecid as offset_src_key_txt,
    vt.invoiceproject as project_contract_orign_ind,
    vt.correct as crrctn_trans_ind,
    vt.settlement as auto_settle_ind,
    vt.cancel as cancelled_ind,
    vt.paymreference as pymnt_ref_txt,
    vt.paymid as pymnt_id,
    vt.amountcur as trans_currency_trans_amt,
    vt.settleamountcur as trans_currency_settled_amt,
    vt.amountmst as opco_currency_trans_amt,
    vt.settleamountmst as opco_currency_settled_amt,
    vt.exchrate as exchg_rt,
    vt.lastexchadj as ltst_exchg_adjstmt_dt,
    vt.exchadjustment as opco_currency_exchg_adjstmt_amt,
    vt.vendexchadjustmentrealized as opco_currency_realized_exchg_adjstmt_amt,
    vt.tax1099amount as opco_currency_tax_amt,
    vt.settletax1099amount as opco_currency_settled_tax_amt,
    vt.discdate_opi as dscnt_dt,
    vt.discamt_opi as dscnt_amt
    from {{ source('AX_DEV', 'VENDTRANS') }} vt 
    left join {{ ref('ax_opco_curr')}} opco1 
        on vt.lastsettlecompany = opco1.opco_id
        and opco1.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_curr')}} opco
        on vt.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_vendor_curr')}} ov 
        on upper(vt.accountnum) = ov.vendor_id
        and ov.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_vendor_curr')}} ov1 
        on upper(vt.lastsettleaccountnum) = ov1.vendor_id
        and ov1.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_cost_center_curr')}} occ 
        on upper(vt.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_dept_curr')}} od 
        on upper(vt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_type_curr')}} ot 
        on upper(vt.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_purpose_curr')}} op 
        on upper(vt.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_lob_curr')}} ol 
        on upper(vt.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_po_curr')}} opo 
        on vt.dataareaid = opo.opco_id 
        and vt.purchid_opi = opo.po_id
        and opo.src_sys_nm = 'AX'
        and vt.purchid_opi is not null
        and vt.purchid_opi <> '0'
    left join {{ ref('ax_opco_gl_trans_type_curr')}} ogt 
        on cast(vt.transtype as string) = cast(ogt.src_gl_trans_type_cd as string)
        and ogt.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_currency_curr')}} oc 
        on upper(vt.currencycode) = oc.src_currency_cd
        and oc.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_pymnt_mode_curr')}} opm 
        on upper(vt.paymmode) = opm.src_pymnt_mode_cd
        and opm.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_cash_dscnt_terms_curr')}} ocd 
        on upper(vt.cashdisccode) = ocd.src_cash_dscnt_terms_cd
        and ocd.src_sys_nm = 'AX'
    where vt.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_key_txt']) }} as opco_vendor_trans_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_key_txt', 'opco_sk', 'opco_vendor_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_po_sk', 'opco_gl_trans_type_sk', 'trans_currency_sk', 'opco_pymnt_mode_sk', 'opco_cash_dscnt_terms_sk', 'ltst_settled_opco_sk', 'ltst_settled_vendor_sk', 'trans_desc', 'trans_dt', 'due_dt', 'trans_close_dt', 'voucher_nbr', 'invoice_nbr', 'ltst_settled_voucher_nbr', 'ltst_settle_dt', 'trans_apprvd_ind', 'trans_apprvd_by_txt', 'trans_apprvd_dtm', 'document_nbr', 'document_dt', 'offset_src_key_txt', 'project_contract_orign_ind', 'crrctn_trans_ind', 'auto_settle_ind', 'cancelled_ind', 'pymnt_ref_txt', 'pymnt_id', 'trans_currency_trans_amt', 'trans_currency_settled_amt', 'opco_currency_trans_amt', 'opco_currency_settled_amt', 'exchg_rt', 'ltst_exchg_adjstmt_dt', 'opco_currency_exchg_adjstmt_amt', 'opco_currency_realized_exchg_adjstmt_amt', 'opco_currency_tax_amt', 'opco_currency_settled_tax_amt', 'dscnt_dt', 'dscnt_amt']) }} as opco_vendor_trans_ck,
            concat_ws('|', src_sys_nm, opco_id, src_key_txt) as opco_vendor_trans_ak,
            * 
    from ax_opco_vendor_trans 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_VENDOR_TRANS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_VENDOR_TRANS') and _load != 3%}
    union
    select
    OPCO_VENDOR_TRANS_sk, OPCO_VENDOR_TRANS_ck, opco_vendor_trans_ak, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, opco_id, src_key_txt, opco_sk, opco_vendor_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_po_sk, opco_gl_trans_type_sk, trans_currency_sk, opco_pymnt_mode_sk, opco_cash_dscnt_terms_sk, ltst_settled_opco_sk, ltst_settled_vendor_sk, trans_desc, trans_dt, due_dt, trans_close_dt, voucher_nbr, invoice_nbr, ltst_settled_voucher_nbr, ltst_settle_dt, trans_apprvd_ind, trans_apprvd_by_txt, trans_apprvd_dtm, document_nbr, document_dt, offset_src_key_txt, project_contract_orign_ind, crrctn_trans_ind, auto_settle_ind, cancelled_ind, pymnt_ref_txt, pymnt_id, trans_currency_trans_amt, trans_currency_settled_amt, opco_currency_trans_amt, opco_currency_settled_amt, exchg_rt, ltst_exchg_adjstmt_dt, opco_currency_exchg_adjstmt_amt, opco_currency_realized_exchg_adjstmt_amt, opco_currency_tax_amt, opco_currency_settled_tax_amt, dscnt_dt, dscnt_amt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_TRANS
    {% if _load == 1 %}
        where OPCO_VENDOR_TRANS_ck not in (select distinct OPCO_VENDOR_TRANS_ck from final)
    {% else %}
        where OPCO_VENDOR_TRANS_ck not in (select distinct OPCO_VENDOR_TRANS_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_TRANS where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_VENDOR_TRANS_ck not in (select distinct OPCO_VENDOR_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_TRANS where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_VENDOR_TRANS_ck in (select distinct OPCO_VENDOR_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_TRANS where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}