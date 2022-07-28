{% set _load = load_type('AX_OPCO_CUST_INVOICE_LINE') %}

with ax_opco_cust_invoice_line as (
    select 
    current_timestamp as crt_dtm,
    cit._fivetran_synced as stg_load_dtm,
    case
        when cit._fivetran_deleted = true then cit._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    cit.dataareaid as opco_id,
    cit.recid as src_key_txt,
    opco.opco_sk,
    oso.opco_sls_ordr_sk,
    oso1.opco_sls_ordr_sk as orig_sls_ordr_sk,
    oi.opco_item_sk,
    ocoa.opco_chart_of_accts_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    ocr.opco_currency_sk as trans_currency_sk,
    ou.opco_uom_sk as sls_uom_sk,
    ou1.opco_uom_sk as gl_prod_uom_sk,
    ou2.opco_uom_sk as gl_sls_uom_sk,
    oa.opco_assctn_sk,
    oirt.opco_invtry_ref_type_sk,
    cit.invoiceid as invoice_nbr,
    cit.linenum as line_nbr,
    cit.inventtransid as invtry_trans_id,
    cit.externalitemid as cust_item_id,
    cit.taxgroup as tax_grp_id,
    cit.taxitemgroup as tax_item_grp_id,
    cit.inventrefid as invtry_ref_id,
    cit.itembomid_opi as item_bom_id,
    cit.name as item_desc,
    cit.salesgroup as sls_grp_txt,
    cit.countryregionofshipment as shpmt_country_nm,
    cit.dlvcountryregionid as dlvry_country_nm,
    cit.dlvstate as dlvry_state_nm,
    cit.dlvcounty as dlvry_county_nm,
    cit.mark_opi as mark_txt,
    cit.groupname_opi as grp_nm,
    cit.dlvdate as ship_dt,
    cit.partdelivery as partial_dlvry_ind,
    cit.risktype_opi as risk_type_ind,
    cit.kits_opi as kits_ind,
    cit.commisscalc as commsn_calc_ind,
    case 
        when cit.deliverytype = 0 then null
        when cit.deliverytype = 1 then 'Direct Delivery'
    end as dlvry_type_txt,
    cit.lineheader as invoice_line_txt,
    cit.special_opi as spcl_item_ind,
    cit.specialinspecial_opi as spcl_in_spcl_ind,
    cit.specialnumber_opi as spcl_item_nbr,
    cit.laborhours_opi as labor_hours_nbr,
    cit.engineeringhours_opi as engineering_hours_nbr,
    cit.weight as item_weight_amt,
    cit.priceunit as per_unit_qty,
    cit.qty as invoice_item_qty,
    cit.qtyordered_opi as ordr_qty,
    cit.qtyphysical as dlvrd_qty,
    cit.qtyprevinvoiced_opi as prev_invoiced_qty,
    cit.qtyremain_opi as remaining_qty,
    cit.inventqty as invtry_units_qty,
    cit.glprodqty_opi as gl_prod_qty,
    cit.glsalesqty_opi as gl_sls_qty,
    cit.sisqty_opi as sis_qty,
    cit.discpercent as dscnt_pct,
    cit.linepercent as line_dscnt_pct,
    cit.overheadcostpct_opi as overhead_cost_pct,
    cit.mc_opi as margin_contribution_pct,
    cit.lineamount as trans_currency_sls_amt,
    cit.lineamountmst as opco_currency_sls_amt,
    cit.lineamounttax as trans_currency_incl_sls_tax_amt,
    cit.lineamounttaxmst as opco_currency_incl_sls_tax_amt,
    cit.sumlinedisc as trans_currency_sum_line_dscnt_amt,
    cit.sumlinediscmst as opco_currency_sum_line_dscnt_amt,
    cit.taxamount as trans_currency_tax_amt,
    cit.taxamountmst as opco_currency_tax_amt,
    cit.commissamountcur as trans_currency_commsn_amt,
    cit.commissamountmst as opco_currency_commsn_amt,
    cit.salesmarkup as sls_markup_amt,
    cit.salesprice as per_unit_sls_price_amt,
    cit.ediprice_opi as online_price_amt,
    cit.listprice_opi as list_price_amt,
    cit.imbfrtcost_opi as imbedded_freight_cost_amt,
    cit.imbfrt_opi as imbedded_freight_amt,
    cit.totalcost_opi as total_cost_amt,
    cit.itemcost_opi as item_cost_amt,
    cit.defaultprice_opi as default_item_price_amt,
    cit.itemprice_opi as item_price_amt
    from {{ source('AX_DEV', 'CUSTINVOICETRANS') }} cit 
    left join {{ ref('opco')}} opco 
        on cit.dataareaid = opco.opco_id 
        and opco.src_sys_nm = 'AX'
    left join {{ ref('opco_sls_ordr')}} oso 
        on cit.dataareaid = oso.opco_id
        and cit.salesid = oso.sls_ordr_id
        and oso.src_sys_nm = 'AX'
    left join {{ ref('opco_sls_ordr')}} oso1 
        on cit.dataareaid = oso1.opco_id
        and cit.origsalesid = oso1.sls_ordr_id
        and oso1.src_sys_nm = 'AX'
    left join {{ ref('opco_item')}} oi 
        on cit.dataareaid = oi.opco_id
        and cit.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join {{ ref('opco_chart_of_accts')}} ocoa 
        on cit.dataareaid = ocoa.opco_id
        and cit.ledgeraccount = ocoa.gl_acct_nbr
        and ocoa.src_sys_nm = 'AX'
    left join {{ref('opco_cost_center')}} occ 
        on upper(cit.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ref('opco_dept')}} od 
        on upper(cit.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ref('opco_type')}} ot 
        on upper(cit.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ref('opco_purpose')}} op 
        on upper(cit.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join {{ref('opco_lob')}} ol
        on upper(cit.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('opco_currency')}} ocr 
        on upper(cit.currencycode) = ocr.src_currency_cd
        and ocr.src_sys_nm = 'AX'
    left join {{ ref('opco_uom')}} ou 
        on upper(cit.salesunit) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join {{ ref('opco_uom')}} ou1 
        on upper(cit.glproduom_opi) = ou1.src_uom_cd
        and ou1.src_sys_nm = 'AX'
    left join {{ ref('opco_uom')}} ou2 
        on upper(cit.glsalesuom_opi) = ou2.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join {{ ref('opco_assctn')}} oa 
        on cit.dataareaid = oa.opco_id
        and cit.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    left join {{ ref('opco_invtry_ref_type')}} oirt
        on cit.inventreftype = oirt.invtry_ref_type_cd
        and oirt.src_sys_nm = 'AX'
    where cit.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_key_txt']) }} as opco_cust_invoice_line_sk,
           {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_key_txt', 'opco_sk', 'opco_sls_ordr_sk', 'orig_sls_ordr_sk', 'opco_item_sk', 'opco_chart_of_accts_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'trans_currency_sk', 'sls_uom_sk', 'gl_prod_uom_sk', 'gl_sls_uom_sk', 'opco_assctn_sk', 'opco_invtry_ref_type_sk', 'invoice_nbr', 'line_nbr', 'invtry_trans_id', 'cust_item_id', 'tax_grp_id', 'tax_item_grp_id', 'invtry_ref_id', 'item_bom_id', 'item_desc', 'sls_grp_txt', 'shpmt_country_nm', 'dlvry_country_nm', 'dlvry_state_nm', 'dlvry_county_nm', 'mark_txt', 'grp_nm', 'ship_dt', 'partial_dlvry_ind', 'risk_type_ind', 'kits_ind', 'commsn_calc_ind', 'dlvry_type_txt', 'invoice_line_txt', 'spcl_item_ind', 'spcl_in_spcl_ind', 'spcl_item_nbr', 'labor_hours_nbr', 'engineering_hours_nbr', 'item_weight_amt', 'per_unit_qty', 'invoice_item_qty', 'ordr_qty', 'dlvrd_qty', 'prev_invoiced_qty', 'remaining_qty', 'invtry_units_qty', 'gl_prod_qty', 'gl_sls_qty', 'sis_qty', 'dscnt_pct', 'line_dscnt_pct', 'overhead_cost_pct', 'margin_contribution_pct', 'trans_currency_sls_amt', 'opco_currency_sls_amt', 'trans_currency_incl_sls_tax_amt', 'opco_currency_incl_sls_tax_amt', 'trans_currency_sum_line_dscnt_amt', 'opco_currency_sum_line_dscnt_amt', 'trans_currency_tax_amt', 'opco_currency_tax_amt', 'trans_currency_commsn_amt', 'opco_currency_commsn_amt', 'sls_markup_amt', 'per_unit_sls_price_amt', 'online_price_amt', 'list_price_amt', 'imbedded_freight_cost_amt', 'imbedded_freight_amt', 'total_cost_amt', 'item_cost_amt', 'default_item_price_amt', 'item_price_amt']) }} as opco_cust_invoice_line_ck,
            concat_ws('|', src_sys_nm, opco_id, src_key_txt) as opco_cust_invoice_line_ak,
            * 
    from ax_opco_cust_invoice_line 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_CUST_INVOICE_LINE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CUST_INVOICE_LINE') and _load != 3%}
    union
    select
    opco_cust_invoice_line_sk, opco_cust_invoice_line_ck, opco_cust_invoice_line_ak, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, opco_id, src_key_txt, opco_sk, opco_sls_ordr_sk, orig_sls_ordr_sk, opco_item_sk, opco_chart_of_accts_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk,
    opco_lob_sk, trans_currency_sk, sls_uom_sk, gl_prod_uom_sk,  gl_sls_uom_sk, opco_assctn_sk, opco_invtry_ref_type_sk, invoice_nbr, line_nbr, invtry_trans_id,  cust_item_id, tax_grp_id,
    tax_item_grp_id, invtry_ref_id, item_bom_id, item_desc, sls_grp_txt, shpmt_country_nm, dlvry_country_nm, dlvry_state_nm,  dlvry_county_nm, mark_txt, grp_nm, ship_dt, partial_dlvry_ind,
    risk_type_ind, kits_ind, commsn_calc_ind, dlvry_type_txt, invoice_line_txt, spcl_item_ind, spcl_in_spcl_ind, spcl_item_nbr, labor_hours_nbr, engineering_hours_nbr, item_weight_amt,
    per_unit_qty, invoice_item_qty, ordr_qty, dlvrd_qty, prev_invoiced_qty, remaining_qty, invtry_units_qty, gl_prod_qty, gl_sls_qty, sis_qty, dscnt_pct, line_dscnt_pct, 
    overhead_cost_pct, margin_contribution_pct, trans_currency_sls_amt, opco_currency_sls_amt, trans_currency_incl_sls_tax_amt, opco_currency_incl_sls_tax_amt, trans_currency_sum_line_dscnt_amt,
    opco_currency_sum_line_dscnt_amt, trans_currency_tax_amt, opco_currency_tax_amt, trans_currency_commsn_amt, opco_currency_commsn_amt, sls_markup_amt, per_unit_sls_price_amt, 
    online_price_amt, list_price_amt, imbedded_freight_cost_amt, imbedded_freight_amt, total_cost_amt, item_cost_amt, default_item_price_amt, item_price_amt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_invoice_line
    {% if _load == 1 %}
        where OPCO_cust_invoice_line_ck not in (select distinct OPCO_cust_invoice_line_ck from final)
    {% else %}
        where OPCO_cust_invoice_line_ck not in (select distinct OPCO_cust_invoice_line_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_cust_invoice_line where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_cust_invoice_line_ck not in (select distinct OPCO_cust_invoice_line_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_cust_invoice_line where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_cust_invoice_line_ck in (select distinct OPCO_cust_invoice_line_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_cust_invoice_line where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}