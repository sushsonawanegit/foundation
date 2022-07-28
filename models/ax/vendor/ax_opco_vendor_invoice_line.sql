{% set _load = load_type('AX_OPCO_VENDOR_INVOICE_LINE') %}

with ax_opco_vendor_invoice_line as (
    select 
    current_timestamp as crt_dtm,
    vit._fivetran_synced as stg_load_dtm,
    case
        when vit._fivetran_deleted = true then vit._fivetran_synced
        else null 
    end as delete_dtm,
    'AX' as src_sys_nm,
    vit.dataareaid as opco_id,
    vit.invoiceid as invoice_nbr,
    vit.internalinvoiceid as internal_invoice_nbr,
    vit.inventtransid as invtry_trans_id,
    ovi.opco_vendor_invoice_sk,
    opco.opco_sk,
    oi.opco_item_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    opo.opco_po_sk,
    opo1.opco_po_sk as orig_po_sk,
    ocoa.opco_chart_of_accts_sk as gl_acct_sk,
    ocr.opco_currency_sk as trans_currency_sk,
    oa.opco_assctn_sk,
    oirt.opco_invtry_ref_type_sk,
    ou.opco_uom_sk,
    vit.linenum as line_nbr,
    vit.invoicedate as invoice_dt,
    vit.name as item_desc,
    vit.externalitemid as vendor_item_id,
    vit.inventrefid as invtry_ref_id,
    vit.inventreftransid as invtry_ref_trans_id,
    vit.inventdate as invtry_trans_dt,
    vit.destcountryregionid as dest_country_nm,
    vit.deststate as dest_state_nm,
    vit.destcounty as dest_county_nm,
    vit.taxgroup as tax_grp_id,
    vit.taxitemgroup as tax_item_grp_id,
    vit.priceunit as per_unit_qty,
    vit.qty as purchase_qty,
    vit.qtyphysical as received_qty,
    vit.inventqty as invtry_qty,
    vit.discpercent as dscnt_pct,
    vit.linepercent as line_dscnt_pct,
    vit.purchprice as per_unit_price_amt,
    vit.lineamount as trans_currency_trans_amt,
    vit.lineamountmst as opco_currency_trans_amt,
    vit.purchmarkup as trans_currency_misc_chrgs_amt,
    vit.discamount as trans_currency_dscnt_amt,
    vit.linedisc as trans_currency_line_dscnt_amt,
    vit.multilndisc as trans_currency_multi_line_dscnt_amt,
    vit.lineamounttax as trans_currency_tax_amt,
    vit.tax1099amount as opco_currency_tax_1099_amt
    from {{ source('AX_DEV', 'VENDINVOICETRANS') }} vit 
    left join {{ ref('opco_vendor_invoice')}} ovi 
        on vit.dataareaid = ovi.opco_id
        and vit.invoiceid = ovi.invoice_nbr
        and vit.internalinvoiceid = ovi.internal_invoice_nbr
        and ovi.src_sys_nm = 'AX'
    left join {{ ref('opco')}} opco 
        on vit.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ ref('opco_item')}} oi 
        on vit.dataareaid = oi.opco_id
        and vit.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join {{ ref('opco_cost_center')}} occ 
        on upper(vit.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ ref('opco_dept')}} od 
        on upper(vit.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ ref('opco_type')}} ot 
        on upper(vit.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ ref('opco_purpose')}} op 
        on upper(vit.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join {{ ref('opco_lob')}} ol 
        on upper(vit.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('opco_po')}} opo 
        on vit.dataareaid = opo.opco_id 
        and vit.purchid = opo.po_id
        and opo.src_sys_nm = 'AX'
    left join {{ ref('opco_po')}} opo1
        on vit.dataareaid = opo1.opco_id 
        and vit.origpurchid = opo1.po_id
        and opo1.src_sys_nm = 'AX'
    left join {{ ref('opco_chart_of_accts')}} ocoa
        on vit.dataareaid = ocoa.opco_id
        and vit.ledgeraccount = ocoa.gl_acct_nbr
        and ocoa.src_sys_nm = 'AX'
    left join {{ ref('opco_currency')}} ocr 
        on upper(vit.currencycode) = ocr.src_currency_cd
        and ocr.src_sys_nm = 'AX'
    left join {{ ref('opco_assctn')}} oa
        on vit.dataareaid = oa.opco_id
        and vit.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    left join {{ ref('opco_invtry_ref_type')}} oirt 
        on vit.inventreftype = oirt.invtry_ref_type_cd
        and oirt.src_sys_nm = 'AX'
    left join {{ ref('opco_uom')}} ou 
        on upper(vit.purchunit) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    where vit.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'invoice_nbr', 'internal_invoice_nbr', 'invtry_trans_id']) }} as opco_vendor_invoice_line_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'invoice_nbr', 'internal_invoice_nbr', 'invtry_trans_id', 'opco_vendor_invoice_sk', 'opco_sk', 'opco_item_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_po_sk', 'orig_po_sk', 'gl_acct_sk', 'trans_currency_sk', 'opco_assctn_sk', 'opco_invtry_ref_type_sk', 'opco_uom_sk', 'line_nbr', 'invoice_dt', 'item_desc', 'vendor_item_id', 'invtry_ref_id', 'invtry_ref_trans_id', 'invtry_trans_dt', 'dest_country_nm', 'dest_state_nm', 'dest_county_nm', 'tax_grp_id', 'tax_item_grp_id', 'per_unit_qty', 'purchase_qty', 'received_qty', 'invtry_qty', 'dscnt_pct', 'line_dscnt_pct', 'per_unit_price_amt', 'trans_currency_trans_amt', 'opco_currency_trans_amt', 'trans_currency_misc_chrgs_amt', 'trans_currency_dscnt_amt', 'trans_currency_line_dscnt_amt', 'trans_currency_multi_line_dscnt_amt', 'trans_currency_tax_amt', 'opco_currency_tax_1099_amt']) }} as opco_vendor_invoice_line_ck,
            concat_ws('|', src_sys_nm, opco_id, invoice_nbr, internal_invoice_nbr, invtry_trans_id) as opco_vendor_invoice_line_ak,
            * 
    from ax_opco_vendor_invoice_line 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_VENDOR_INVOICE_LINE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_VENDOR_INVOICE_LINE') and _load != 3%}
    union
    select
    OPCO_VENDOR_INVOICE_LINE_sk, OPCO_VENDOR_INVOICE_LINE_ck, opco_vendor_invoice_line_ak, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, opco_id, invoice_nbr, internal_invoice_nbr, invtry_trans_id, opco_vendor_invoice_sk, opco_sk, opco_item_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_po_sk, orig_po_sk, gl_acct_sk, trans_currency_sk, opco_assctn_sk, opco_invtry_ref_type_sk, opco_uom_sk, line_nbr, invoice_dt, item_desc, vendor_item_id, invtry_ref_id, invtry_ref_trans_id, invtry_trans_dt, dest_country_nm, dest_state_nm, dest_county_nm, tax_grp_id, tax_item_grp_id, per_unit_qty, purchase_qty, received_qty, invtry_qty, dscnt_pct, line_dscnt_pct, per_unit_price_amt, trans_currency_trans_amt, opco_currency_trans_amt, trans_currency_misc_chrgs_amt, trans_currency_dscnt_amt, trans_currency_line_dscnt_amt, trans_currency_multi_line_dscnt_amt, trans_currency_tax_amt, opco_currency_tax_1099_amt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_INVOICE_LINE
    {% if _load == 1 %}
        where OPCO_VENDOR_INVOICE_LINE_ck not in (select distinct OPCO_VENDOR_INVOICE_LINE_ck from final)
    {% else %}
        where OPCO_VENDOR_INVOICE_LINE_ck not in (select distinct OPCO_VENDOR_INVOICE_LINE_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_INVOICE_LINE where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_VENDOR_INVOICE_LINE_ck not in (select distinct OPCO_VENDOR_INVOICE_LINE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_INVOICE_LINE where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_VENDOR_INVOICE_LINE_ck in (select distinct OPCO_VENDOR_INVOICE_LINE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_INVOICE_LINE where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}