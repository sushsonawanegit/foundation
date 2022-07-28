{% set _load = load_type('AX_OPCO_VENDOR_INVOICE') %}

with ax_opco_vendor_invoice as(
    select
    current_timestamp as crt_dtm,
    vij._fivetran_synced as stg_load_dtm,
    case
        when vij._fivetran_deleted = true then vij._fivetran_synced 
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    vij.dataareaid as opco_id,
    vij.invoiceid as invoice_nbr,
    vij.internalinvoiceid as internal_invoice_nbr,
    opco.opco_sk,
    opo.opco_po_sk,
    ov.opco_vendor_sk,
    ov1.opco_vendor_sk as opco_invoice_vendor_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    opt.opco_po_type_sk,
    oc.opco_currency_sk as trans_currency_sk,
    opm.opco_pymnt_terms_sk,
    ocd.opco_cash_dscnt_terms_sk,
    odt.opco_dlvry_terms_sk,
    odm.opco_dlvry_mode_sk,
    opr.opco_project_sk,
    vij.vendgroup as vendor_grp_cd,
    vij.invoicedate as invoice_dt,
    vij.duedate as due_dt,
    vij.ledgervoucher as gl_voucher_nbr,
    vij.documentnum as document_nbr,
    vij.documentdate as document_dt,
    vij.cashdiscdate as cash_dscnt_dt,
    vij.countryregionid as default_country_nm,
    vij.purchplacer_opi as purchaser_nm,
    vij.qty as invoice_qty,
    vij.weight as invoice_weight_amt,
    vij.volume as invoice_vol_amt,
    vij.requestedby_opi as rqstd_by_nm,
    vij.exchrate as exchg_rt,
    vij.paymid as pymnt_id,
    vij.taxgroup as tax_grp_id,
    vij.itembuyergroupid as item_buyer_grp_id,
    vij.fixedduedate as fixed_due_dt,
    vij.invoiceamount as trans_currency_invoice_amt,
    vij.invoiceamountmst as opco_currency_invoice_amt,
    vij.invoiceroundoff as trans_currency_invoice_roundoff_amt,
    vij.summarkup as trans_currency_misc_chrgs_amt,
    vij.summarkupmst as opco_currency_misc_chrgs_amt,
    vij.cashdisc as trans_currency_cash_dscnt_amt,
    vij.sumlinedisc as trans_currency_line_dscnt_amt,
    vij.salesbalance as trans_currency_balance_sls_amt    
    from {{ source('AX_DEV', 'VENDINVOICEJOUR') }} vij
    left join {{ ref('opco')}} opco 
        on vij.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ ref('opco_po')}} opo 
        on vij.dataareaid = opo.opco_id
        and vij.purchid = opo.po_id
        and opo.src_sys_nm = 'AX'
    left join {{ ref('opco_vendor')}} ov 
        on upper(vij.orderaccount) = ov.vendor_id
        and ov.src_sys_nm = 'AX'
    left join {{ ref('opco_vendor')}} ov1 
        on upper(vij.invoiceaccount) = ov1.vendor_id
        and ov1.src_sys_nm = 'AX'
    left join {{ ref('opco_cost_center')}} occ 
        on upper(vij.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ ref('opco_dept')}} od 
        on upper(vij.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ ref('opco_type')}} ot 
        on upper(vij.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ ref('opco_purpose')}} op 
        on upper(vij.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join {{ ref('opco_lob')}} ol 
        on upper(vij.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('opco_po_type')}} opt
        on vij.purchasetype = opt.src_po_type_cd
        and opt.src_sys_nm = 'AX'
    left join {{ ref('opco_currency')}} oc 
        on upper(vij.currencycode) = oc.src_currency_cd
        and oc.src_sys_nm = 'AX'
    left join {{ ref('opco_pymnt_terms')}} opm 
        on upper(vij.payment) = opm.src_pymnt_terms_cd
        and opm.src_sys_nm = 'AX'
    left join {{ ref('opco_cash_dscnt_terms')}} ocd 
        on upper(vij.cashdisccode) = ocd.src_cash_dscnt_terms_cd
        and ocd.src_sys_nm = 'AX'
    left join {{ ref('opco_dlvry_terms')}} odt 
        on upper(vij.dlvterm) = odt.src_dlvry_terms_cd
        and odt.src_sys_nm = 'AX'
    left join {{ ref('opco_dlvry_mode')}} odm 
        on upper(vij.dlvmode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    left join {{ ref('opco_project')}} opr 
        on vij.dataareaid = opr.opco_id
        and vij.projid_opi = opr.project_id
        and opr.src_sys_nm = 'AX'
    where vij.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'invoice_nbr', 'internal_invoice_nbr']) }} as opco_vendor_invoice_sk, 
            {{ dbt_utils.surrogate_key([ 'src_sys_nm' , 'opco_id' , 'invoice_nbr' , 'internal_invoice_nbr' , 'opco_sk' , 'opco_po_sk' , 'opco_vendor_sk' , 'opco_invoice_vendor_sk' , 'opco_cost_center_sk' , 'opco_dept_sk' , 'opco_type_sk' , 'opco_purpose_sk' , 'opco_lob_sk' , 'opco_po_type_sk' , 'trans_currency_sk' , 'opco_pymnt_terms_sk' , 'opco_cash_dscnt_terms_sk' , 'opco_dlvry_terms_sk' , 'opco_dlvry_mode_sk' , 'opco_project_sk' , 'vendor_grp_cd' , 'invoice_dt' , 'due_dt' , 'gl_voucher_nbr' , 'document_nbr' , 'document_dt' , 'cash_dscnt_dt' , 'default_country_nm' , 'purchaser_nm' , 'invoice_qty' , 'invoice_weight_amt' , 'invoice_vol_amt' , 'rqstd_by_nm' , 'exchg_rt' , 'pymnt_id' , 'tax_grp_id' , 'item_buyer_grp_id' , 'fixed_due_dt' , 'trans_currency_invoice_amt' , 'opco_currency_invoice_amt' , 'trans_currency_invoice_roundoff_amt' , 'trans_currency_misc_chrgs_amt' , 'opco_currency_misc_chrgs_amt' , 'trans_currency_cash_dscnt_amt' , 'trans_currency_line_dscnt_amt' , 'trans_currency_balance_sls_amt']) }} as opco_vendor_invoice_ck,
            concat_ws('|', src_sys_nm, opco_id, invoice_nbr, internal_invoice_nbr) as opco_vendor_invoice_ak,
            * 
    from ax_opco_vendor_invoice 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_VENDOR_INVOICE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_VENDOR_INVOICE') and _load != 3%}
    union
    select
    OPCO_VENDOR_INVOICE_sk, OPCO_VENDOR_INVOICE_ck, opco_vendor_invoice_ak, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, opco_id, invoice_nbr, internal_invoice_nbr, opco_sk, opco_po_sk, opco_vendor_sk, opco_invoice_vendor_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_po_type_sk, trans_currency_sk, opco_pymnt_terms_sk, opco_cash_dscnt_terms_sk, opco_dlvry_terms_sk, opco_dlvry_mode_sk, opco_project_sk, vendor_grp_cd, invoice_dt, due_dt, gl_voucher_nbr, document_nbr, document_dt, cash_dscnt_dt, default_country_nm, purchaser_nm, invoice_qty, invoice_weight_amt, invoice_vol_amt, rqstd_by_nm, exchg_rt, pymnt_id, tax_grp_id, item_buyer_grp_id, fixed_due_dt, trans_currency_invoice_amt, opco_currency_invoice_amt, trans_currency_invoice_roundoff_amt, trans_currency_misc_chrgs_amt, opco_currency_misc_chrgs_amt, trans_currency_cash_dscnt_amt, trans_currency_line_dscnt_amt, trans_currency_balance_sls_amt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_INVOICE
    {% if _load == 1 %}
        where OPCO_VENDOR_INVOICE_ck not in (select distinct OPCO_VENDOR_INVOICE_ck from final)
    {% else %}
        where OPCO_VENDOR_INVOICE_ck not in (select distinct OPCO_VENDOR_INVOICE_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_INVOICE where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_VENDOR_INVOICE_ck not in (select distinct OPCO_VENDOR_INVOICE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_INVOICE where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_VENDOR_INVOICE_ck in (select distinct OPCO_VENDOR_INVOICE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_INVOICE where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}