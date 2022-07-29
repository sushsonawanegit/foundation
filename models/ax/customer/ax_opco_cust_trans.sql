{% set _load = load_type('AX_OPCO_CUST_TRANS') %}

with ax_opco_cust_trans as (
    select
    current_timestamp as crt_dtm,
    ct._fivetran_synced as stg_load_dtm,
    case 
        when ct._fivetran_deleted = true then ct._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    ct.dataareaid as opco_id,
    ct.recid as src_key_txt,
    opco.opco_sk,
    oc.opco_cust_sk,
    odc.opco_cust_sk as order_cust_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    oso.opco_sls_ordr_sk,
    ogt.opco_gl_trans_type_sk,
    ocu.opco_currency_sk as trans_currency_sk,
    opm.opco_pymnt_mode_sk,
    opt.opco_pymnt_terms_sk,
    ocd.opco_cash_dscnt_terms_sk,
    odm.opco_dlvry_mode_sk,
    opr.opco_project_sk,
    ct.transdate as trans_dt,
    ct.approved as trans_apprvd_ind,
    ct.voucher as voucher_nbr,
    ct.invoice as invoice_nbr,
    ct.offsetrecid as offset_src_key_txt,
    ct.duedate as due_dt,
    ct.correct as crrctn_trans_ind,
    ct.settlement as auto_settle_ind,
    ct.cancelledpayment as pymnt_cancelled_ind,
    ct.interest as interest_calc_allowed_ind,
    ct.collectionletter as collection_letter_allowed_ind,
    ct.cashpayment as cod_cash_pymnt_ind,
    ct.paymreference as pymnt_ref_txt,
    case 
        when ct.paymmethod = 0 then 'Net'
        when ct.paymmethod = 1 then 'Current Month'
        when ct.paymmethod = 2 then 'Current Quarter'
        when ct.paymmethod = 3 then 'Current Year'
        when ct.paymmethod = 4 then 'Current Week'
        when ct.paymmethod = 5 then 'C.O.D.'
    end as pymnt_method_txt,
    ct.retention_opi as retention_cd,
    ct.retentioncheck_opi as check_retention_ind,
    ct.documentnum as document_nbr,
    ct.documentdate as document_dt,
    ct.discdate_opi as trans_dscnt_dt,
    ct.lastinterestdate_opi as ltst_interest_dt,
    case
        when ct.collectionlettercode = 0 then 'None'
        when ct.collectionlettercode = 1 then 'Collection Letter 1'
        when ct.collectionlettercode = 2 then 'Collection Letter 2'
        when ct.collectionlettercode = 3 then 'Collection Letter 3'
        when ct.collectionlettercode = 4 then 'Collection Letter 4'
        when ct.collectionlettercode = 5 then 'Collection'
        when ct.collectionlettercode = 6 then 'All'
        when ct.collectionlettercode = 7 then 'Collection Per Customer'
    end as collection_letter_txt,
    ct.purchaseorder_opi as po_form_nbr,
    ct.lastsettlecompany as ltst_settled_opco_sk,
    ct.lastsettleaccountnum as ltst_settled_cust_sk,
    ct.lastsettledate as ltst_settled_dt,
    ct.lastsettlevoucher as ltst_settled_voucher_nbr,
    ct.closed as trans_closed_dt,
    ct.amountcur as trans_currency_trans_amt,
    ct.amountmst as opco_currency_trans_amt,
    ct.settleamountcur as trans_currency_total_settled_amt,
    ct.settleamountmst as opco_currency_total_settled_amt,
    ct.discamt_opi as trans_dscnt_amt,
    ct.sumtax_opi as sum_tax_amt,
    ct.exchrate as trans_exch_rt
    from {{ source('AX_DEV', 'CUSTTRANS') }} ct 
    left join {{ ref('v_ax_opco_curr')}} opco 
        on ct.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_cust_curr')}} oc 
        on ct.accountnum = oc.cust_id
        and ct.dataareaid = oc.opco_id
        and oc.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_cust_curr')}} odc 
        on ct.orderaccount = odc.cust_id
        and ct.dataareaid = odc.opco_id
        and odc.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_cost_center_curr')}} occ 
        on upper(ct.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_dept_curr')}} od 
        on upper(ct.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_type_curr')}} ot 
        on upper(ct.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_purpose_curr')}} op 
        on upper(ct.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_lob_curr')}} ol 
        on upper(ct.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_sls_ordr_curr')}} oso 
        on ct.dataareaid = oso.opco_id
        and ct.salesid_opi = oso.sls_ordr_id
        and oso.src_sys_nm = 'AX'
        and ct.salesid_opi is not null
        and ct.salesid_opi <> '0'
    left join {{ ref('v_ax_opco_gl_trans_type_curr')}} ogt 
        on ct.transtype::string =  ogt.src_gl_trans_type_cd
        and ogt.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_currency_curr')}} ocu 
        on upper(ct.currencycode) = ocu.src_currency_cd
        and ocu.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_pymnt_mode_curr')}} opm 
        on upper(ct.paymmode) = opm.src_pymnt_mode_cd
        and opm.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_cash_dscnt_terms_curr')}} ocd
        on upper(ct.cashdisccode) = ocd.src_cash_dscnt_terms_cd
        and ocd.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_pymnt_terms_curr')}} opt 
        on upper(ct.payment_opi) = opt.src_pymnt_terms_cd
        and opt.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_dlvry_mode_curr')}} odm 
        on upper(ct.deliverymode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_project_curr')}} opr 
        on ct.projid_opi = opr.project_id
        and ct.dataareaid = opr.opco_id
        and opr.src_sys_nm = 'AX'
    where ct.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as(
    select distinct
           {{dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_key_txt'])}} as opco_cust_trans_sk,
           {{dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_key_txt', 'opco_sk', 'opco_cust_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_sls_ordr_sk', 'opco_gl_trans_type_sk', 'trans_currency_sk', 'opco_pymnt_mode_sk', 'opco_pymnt_terms_sk', 'opco_cash_dscnt_terms_sk', 'opco_dlvry_mode_sk', 'opco_project_sk', 'trans_dt', 'trans_apprvd_ind', 'voucher_nbr', 'invoice_nbr', 'offset_src_key_txt', 'due_dt', 'crrctn_trans_ind', 'auto_settle_ind', 'pymnt_cancelled_ind', 'interest_calc_allowed_ind', 'collection_letter_allowed_ind', 'cod_cash_pymnt_ind', 'pymnt_ref_txt', 'pymnt_method_txt', 'retention_cd', 'check_retention_ind', 'document_nbr', 'document_dt', 'trans_dscnt_dt', 'ltst_interest_dt', 'collection_letter_txt', 'po_form_nbr', 'ltst_settled_opco_sk', 'ltst_settled_cust_sk', 'ltst_settled_dt', 'ltst_settled_voucher_nbr', 'trans_closed_dt', 'trans_currency_trans_amt', 'opco_currency_trans_amt', 'trans_currency_total_settled_amt', 'opco_currency_total_settled_amt', 'trans_dscnt_amt', 'sum_tax_amt', 'trans_exch_rt'])}} as opco_cust_trans_ck,
           concat_ws('|', src_sys_nm, opco_id, src_key_txt) as opco_cust_trans_ak, *
    from ax_opco_cust_trans 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_CUST_TRANS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CUST_TRANS') and _load != 3%}
    union
    select
    opco_cust_trans_sk, opco_cust_trans_ck, opco_cust_trans_ak, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, opco_id, src_key_txt, opco_sk, opco_cust_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_sls_ordr_sk, opco_gl_trans_type_sk,
    trans_currency_sk, opco_pymnt_mode_sk, opco_pymnt_terms_sk, opco_cash_dscnt_terms_sk, opco_dlvry_mode_sk, opco_project_sk, trans_dt, trans_apprvd_ind,
    voucher_nbr, invoice_nbr, offset_src_key_txt, due_dt, crrctn_trans_ind, auto_settle_ind, pymnt_cancelled_ind, interest_calc_allowed_ind, collection_letter_allowed_ind,
    cod_cash_pymnt_ind, pymnt_ref_txt, pymnt_method_txt, retention_cd, check_retention_ind, document_nbr, document_dt, trans_dscnt_dt, ltst_interest_dt,
    collection_letter_txt, po_form_nbr, ltst_settled_opco_sk, ltst_settled_cust_sk, ltst_settled_dt, ltst_settled_voucher_nbr, trans_closed_dt, trans_currency_trans_amt,
    opco_currency_trans_amt, trans_currency_total_settled_amt, opco_currency_total_settled_amt, trans_dscnt_amt, sum_tax_amt, trans_exch_rt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_trans
    {% if _load == 1 %}
        where opco_cust_trans_ck not in (select distinct opco_cust_trans_ck from final)
    {% else %}
        where opco_cust_trans_ck not in (select distinct opco_cust_trans_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_trans where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_trans_ck not in (select distinct opco_cust_trans_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_trans where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_trans_ck in (select distinct opco_cust_trans_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_trans where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}