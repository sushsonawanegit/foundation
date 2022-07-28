{% set _load = load_type('AX_OPCO_PO') %}

with ax_opco_po as (
    select 
    current_timestamp as crt_dtm,
    pt._fivetran_synced as stg_load_dtm,
    case 
        when pt._fivetran_deleted = true then pt._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    pt.dataareaid as opco_id,
    pt.purchid as po_id,
    pt.purchname as po_nm,
    opco.opco_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    oip.opco_purpose_sk,
    ol.opco_lob_sk,
    ov.opco_vendor_sk,
    ov2.opco_vendor_sk as invoice_vendor_sk,
    ocd.opco_cash_dscnt_terms_sk,
    oc.opco_currency_sk as trans_currency_sk,
    ots.opco_trans_status_sk,
    opo.opco_po_type_sk,
    opt.opco_pymnt_terms_sk,
    opm.opco_pymnt_mode_sk,
    oso.opco_sls_ordr_sk as intercompany_orig_sls_ordr_sk,
    ocu.opco_cust_sk as intercompany_orig_cust_sk,
    odm.opco_dlvry_mode_sk,
    odt.opco_dlvry_terms_sk,
    opds.opco_po_doc_status_sk,
    wr.warehouse_sk,
    op.opco_project_sk,
    oln.opco_locn_sk,
    pt.deliverydate as dlvry_dt,
    pt.createddatetime as po_crt_dtm,
    pt.requestedby_opi as po_reqstd_by_nm,
    upper(pt.vendgroup) as vendor_grp_cd,
    pt.deliveryname as dlvry_vendor_nm,
    pt.vendorref as vendor_ref_txt,
    pt.namealias_opi as alias_nm,
    pt.itembuyergroupid as item_buyer_grp_id,
    pt.purchplacer as purchased_by_txt,
    pt.taxable_opi as taxable_ind,
    pt.discpercent as dscnt_pct,
    pt.cashdiscpercent as cash_dscnt_pct
    from {{ source('AX_DEV', 'PURCHTABLE') }} pt
    left join {{ ref('opco')}} opco 
        on pt.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ ref('opco_cost_center')}} occ 
        on upper(pt.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ ref('opco_dept')}} od 
        on upper(pt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ ref('opco_type')}} ot 
        on upper(pt.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ ref('opco_purpose')}} oip
        on upper(pt.dimension4_) = oip.src_purpose_cd
        and oip.src_sys_nm = 'AX'
    left join {{ ref('opco_lob')}} ol
        on upper(pt.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('opco_vendor')}} ov 
        on upper(pt.orderaccount) = ov.vendor_id
        and ov.src_sys_nm = 'AX'
    left join {{ ref('opco_vendor')}} ov2
        on upper(pt.invoiceaccount) = ov2.vendor_id
        and ov2.src_sys_nm = 'AX'
    left join {{ ref('opco_cash_dscnt_terms')}} ocd 
        on upper(pt.cashdisc) = ocd.src_cash_dscnt_terms_cd
        and ocd.src_sys_nm = 'AX'
    left join {{ ref('opco_currency')}} oc 
        on upper(pt.currencycode) = oc.src_currency_cd
        and oc.src_sys_nm = 'AX'
    left join {{ ref('opco_trans_status')}} ots 
        on pt.purchstatus = ots.src_trans_status_cd
        and ots.src_sys_nm = 'AX'
    left join {{ ref('opco_po_type')}} opo
        on pt.purchasetype = opo.src_po_type_cd
        and opo.src_sys_nm = 'AX'
    left join {{ ref('opco_pymnt_terms')}} opt 
        on upper(pt.payment) = opt.src_pymnt_terms_cd
        and opt.src_sys_nm = 'AX'
    left join {{ ref('opco_pymnt_mode')}} opm 
        on upper(pt.paymmode) = opm.src_pymnt_mode_cd
        and opm.src_sys_nm = 'AX'
    left join {{ ref('opco_cust')}} ocu 
        on pt.dataareaid = ocu.opco_id
        and pt.intercompanyoriginalcustacco12 = ocu.cust_id
        and ocu.src_sys_nm = 'AX'
    left join {{ ref('opco_sls_ordr')}} oso 
        on pt.dataareaid = oso.opco_id
        and pt.intercompanyoriginalsalesid = oso.sls_ordr_id
        and oso.src_sys_nm = 'AX'
    left join {{ ref('opco_dlvry_mode')}} odm 
        on upper(pt.dlvmode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    left join {{ ref('opco_dlvry_terms')}} odt 
        on upper(pt.dlvterm) = odt.src_dlvry_terms_cd
        and odt.src_sys_nm = 'AX'
    left join {{ ref('opco_po_doc_status')}} opds
        on pt.documentstatus = opds.po_doc_status_cd
        and opds.src_sys_nm = 'AX'
    left join {{ ref('warehouse')}} wr 
        on pt.dataareaid = wr.opco_id
        and upper(pt.inventlocationid) = wr.warehouse_id
        and wr.src_sys_nm = 'AX'
    left join {{ ref('opco_project')}} op
        on pt.dataareaid = op.opco_id
        and pt.projid = op.project_id
        and op.src_sys_nm = 'AX'
    left join {{ ref('opco_locn')}} oln 
        on upper(pt.deliverystreet) = oln.addr_ln_1_txt
        and upper(pt.deliverycity) = oln.city_nm
        and upper(pt.dlvstate) = oln.state_nm
        and upper(pt.dlvcountryregionid) = oln.country_nm
        and upper(pt.deliveryzipcode) = oln.zip_cd
        and oln.src_sys_nm = 'AX'
    where pt.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select distinct 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'po_id']) }} as opco_po_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'po_id', 'po_nm', 'opco_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_vendor_sk', 'invoice_vendor_sk', 'opco_cash_dscnt_terms_sk', 'trans_currency_sk', 'opco_trans_status_sk', 'opco_po_type_sk', 'opco_pymnt_terms_sk', 'opco_pymnt_mode_sk', 'intercompany_orig_sls_ordr_sk', 'intercompany_orig_cust_sk', 'opco_dlvry_mode_sk', 'opco_dlvry_terms_sk', 'opco_po_doc_status_sk', 'warehouse_sk', 'opco_project_sk', 'opco_locn_sk', 'dlvry_dt', 'po_crt_dtm', 'po_reqstd_by_nm', 'vendor_grp_cd', 'dlvry_vendor_nm', 'vendor_ref_txt', 'alias_nm', 'item_buyer_grp_id', 'purchased_by_txt', 'taxable_ind', 'dscnt_pct', 'cash_dscnt_pct']) }} as opco_po_ck,
            concat_ws('|', src_sys_nm, opco_id, po_id) as opco_po_ak,
            * 
    from ax_opco_po 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_PO') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PO') and _load != 3%}
    union
    select
    opco_po_sk, opco_po_ck,opco_po_ak, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, opco_id, po_id, po_nm, opco_sk, opco_cost_center_sk, opco_dept_sk,opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_vendor_sk,
    invoice_vendor_sk, opco_cash_dscnt_terms_sk, trans_currency_sk, opco_trans_status_sk, opco_po_type_sk, opco_pymnt_terms_sk, opco_pymnt_mode_sk,
    intercompany_orig_sls_ordr_sk, intercompany_orig_cust_sk, opco_dlvry_mode_sk,  opco_dlvry_terms_sk, opco_po_doc_status_sk, warehouse_sk,
    opco_project_sk, opco_locn_sk, dlvry_dt, po_crt_dtm, po_reqstd_by_nm, vendor_grp_cd, dlvry_vendor_nm, vendor_ref_txt, alias_nm, item_buyer_grp_id,
    purchased_by_txt, taxable_ind, dscnt_pct, cash_dscnt_pct
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PO
    {% if _load == 1 %}
        where opco_po_ck not in (select distinct opco_po_ck from final)
    {% else %}
        where opco_po_ck not in (select distinct opco_po_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PO where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_po_ck not in (select distinct opco_po_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PO where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_po_ck in (select distinct opco_po_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PO where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  