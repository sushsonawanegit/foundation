{% set _load = load_type('AX_OPCO_SLS_ORDR_LINE') %}

with ax_opco_sls_ordr_line as (
    select 
    current_timestamp as crt_dtm,
    sl._fivetran_synced as stg_load_dtm,
    case
        when sl._fivetran_deleted = true then sl._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    sl.dataareaid as opco_id,
    sl.salesid as sls_ordr_id,
    sl.linenum as sls_ordr_line_nbr,
    sl.inventtransid as invtry_trans_id,
    opco.opco_sk,
    oso.opco_sls_ordr_sk,
    oi.opco_item_sk,
    oc.opco_cust_sk,
    ocu.opco_currency_sk as trans_currency_sk,
    ou.opco_uom_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    oip.opco_purpose_sk,
    ol.opco_lob_sk,
    oa.opco_assctn_sk,
    osos.opco_trans_status_sk,
    osot.opco_sls_ordr_type_sk, 
    odm.opco_dlvry_mode_sk,
    sl.blocked as line_blocked_ind,
    sl.complete as line_dlvry_complete_ind,
    sl.name as item_nm,
    case
        when sl.deliverytype = 0 then null
        when sl.deliverytype = 1 then 'Direct Delivery'
    end as dlvry_type_txt,
    sl.purchorderformnum as po_form_nbr,
    sl.drawing_opi as drawing_locn_txt,
    sl.itembomid as item_bom_id,
    sl.deliverydatecontroltype as dlvry_date_contrl_txt,
    sl.createddatetime as sls_ordr_line_crt_dtm,
    sl.receiptdaterequested as rqst_rcpt_dt,
    sl.receiptdateconfirmed as cnfrm_rcpt_dt,
    sl.shippingdaterequested as rqst_ship_dt,
    sl.shippingdateconfirmed as cnfrm_ship_dt,
    sl.confirmeddlv as cnfrm_dlvry_dt,
    sl.groupname_opi as grp_nm,
    sl.inventrefid as trans_ref_id,
    sl.mark_opi as mrkt_cd,
    sl.orderdetailid_opi as sls_ordr_dtl_id,
    sl.phaseid_opi as phase_id,
    sl.priority_opi as priority_cd,
    sl.risktype_opi as risk_ind,
    sl.special_opi as spcl_item_ind,
    sl.specialinspecial_opi as spcl_in_spcl_item_ind,
    sl.specialnumber_opi as spcl_item_id,
    sl.titanjobnum_opi as titan_job_nbr,
    sl.taxgroup as tax_grp_id,
    sl.taxitemgroup as tax_item_grp_txt,
    sl.salesgroup as sls_grp_txt,
    sl.salesunit as sales_unit_cd,
    sl.height_opi as item_height_nbr,
    sl.itemnetweight_opi as item_net_weight_amt,
    sl.remaininventphysical as invtry_qty,
    sl.qtyordered as ordr_qty,
    sl.remainsalesphysical as non_dlvrd_qty,
    sl.remainsalesfinancial as non_invoiced_dlvrd_qty,
    sl.salesqty as sls_qty,
    sl.sisqty_opi as sis_qty,
    sl.costprice as unit_cost_price_amt,
    sl.salesprice as unit_sls_price_amt,
    sl.defaultprice_opi as item_default_price_amt,
    sl.ediprice_opi as item_online_price_amt,
    sl.listprice_opi as item_list_price_amt,
    sl.itemcost_opi as item_cost_amt,
    sl.lineamount as total_sls_amt,
    sl.salesmarkup as fixed_misc_amt,
    sl.imbfrt_opi as imbedded_freight_amt,
    sl.imbfrtcost_opi as imbedded_freight_cost_amt,
    sl.overheadcostpct_opi as overhead_cost_pct,
    sl.linepercent as line_dscnt_pct,
    sl.engineeringhours_opi as engineering_hours_nbr,
    sl.laborhours_opi as labor_hours_nbr   
    from {{source('AX_DEV', 'SALESLINE')}} sl 
    left join {{ref('opco')}} opco 
        on sl.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ref('opco_sls_ordr')}} oso 
        on sl.dataareaid = oso.opco_id
        and sl.salesid = oso.sls_ordr_id
        and oso.src_sys_nm = 'AX'
    left join {{ref('opco_item')}} oi 
        on sl.dataareaid = oi.opco_id
        and sl.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join {{ref('opco_cust')}} oc 
        on sl.dataareaid = oc.opco_id
        and sl.custaccount = oc.cust_id
        and oc.src_sys_nm = 'AX'
    left join {{ref('opco_currency')}} ocu 
        on upper(sl.currencycode) = ocu.src_currency_cd
        and ocu.src_sys_nm = 'AX'
    left join {{ref('opco_uom')}} ou 
        on upper(sl.salesunit) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join {{ref('opco_cost_center')}} occ 
        on upper(sl.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ref('opco_dept')}} od 
        on upper(sl.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ref('opco_type')}} ot 
        on upper(sl.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ref('opco_purpose')}} oip 
        on upper(sl.dimension4_) = oip.src_purpose_cd
        and oip.src_sys_nm = 'AX'
    left join {{ref('opco_lob')}} ol 
        on upper(sl.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ref('opco_assctn')}} oa 
        on sl.inventdimid = oa.src_assctn_cd
        and sl.dataareaid = oa.opco_id
        and oa.src_sys_nm = 'AX'
    left join {{ref('opco_sls_ordr_type')}} osot 
        on sl.salestype = osot.src_sls_ordr_type_cd
        and osot.src_sys_nm = 'AX'
    left join {{ref('opco_trans_status')}} osos
        on sl.salesstatus = osos.src_trans_status_cd
        and osos.src_sys_nm = 'AX'
    left join {{ ref('opco_dlvry_mode')}} odm 
        on upper(sl.dlvmode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    where sl.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select distinct
           {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'sls_ordr_id', 'sls_ordr_line_nbr', 'invtry_trans_id']) }} as opco_sls_ordr_line_sk,
           {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'sls_ordr_id', 'sls_ordr_line_nbr', 'invtry_trans_id', 'opco_sk', 'opco_sls_ordr_sk', 'opco_item_sk', 'opco_cust_sk', 'trans_currency_sk', 'opco_uom_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_assctn_sk', 'opco_trans_status_sk', 'opco_sls_ordr_type_sk', 'opco_dlvry_mode_sk', 'line_blocked_ind', 'line_dlvry_complete_ind', 'item_nm', 'dlvry_type_txt', 'po_form_nbr', 'drawing_locn_txt', 'item_bom_id', 'dlvry_date_contrl_txt', 'sls_ordr_line_crt_dtm', 'rqst_rcpt_dt', 'cnfrm_rcpt_dt', 'rqst_ship_dt', 'cnfrm_ship_dt', 'cnfrm_dlvry_dt', 'grp_nm', 'trans_ref_id', 'mrkt_cd', 'sls_ordr_dtl_id', 'phase_id', 'priority_cd', 'risk_ind', 'spcl_item_ind', 'spcl_in_spcl_item_ind', 'spcl_item_id', 'titan_job_nbr', 'tax_grp_id', 'tax_item_grp_txt', 'sls_grp_txt', 'sales_unit_cd', 'item_height_nbr', 'item_net_weight_amt', 'invtry_qty', 'ordr_qty', 'non_dlvrd_qty', 'non_invoiced_dlvrd_qty', 'sls_qty', 'sis_qty', 'unit_cost_price_amt', 'unit_sls_price_amt', 'item_default_price_amt', 'item_online_price_amt', 'item_list_price_amt', 'item_cost_amt', 'total_sls_amt', 'fixed_misc_amt', 'imbedded_freight_amt', 'imbedded_freight_cost_amt', 'overhead_cost_pct', 'line_dscnt_pct', 'engineering_hours_nbr', 'labor_hours_nbr']) }} as opco_sls_ordr_line_ck,
            concat_ws('|', src_sys_nm, opco_id, sls_ordr_id, sls_ordr_line_nbr, invtry_trans_id) as opco_sls_ordr_line_ak, 
            * 
    from ax_opco_sls_ordr_line 
    
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_SLS_ORDR_LINE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_SLS_ORDR_LINE') and _load != 3%}
    union
    select
    opco_sls_ordr_line_sk, opco_sls_ordr_line_ck, opco_sls_ordr_line_ak, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, opco_id, sls_ordr_id, sls_ordr_line_nbr, invtry_trans_id, opco_sk, opco_sls_ordr_sk, opco_item_sk, opco_cust_sk, trans_currency_sk,
    opco_uom_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_assctn_sk, opco_trans_status_sk, opco_sls_ordr_type_sk,
    opco_dlvry_mode_sk, line_blocked_ind, line_dlvry_complete_ind, item_nm, dlvry_type_txt, po_form_nbr, drawing_locn_txt, item_bom_id, dlvry_date_contrl_txt, 
    sls_ordr_line_crt_dtm, rqst_rcpt_dt, cnfrm_rcpt_dt, rqst_ship_dt, cnfrm_ship_dt, cnfrm_dlvry_dt, grp_nm, trans_ref_id, mrkt_cd, sls_ordr_dtl_id, phase_id, 
    priority_cd, risk_ind, spcl_item_ind, spcl_in_spcl_item_ind, spcl_item_id, titan_job_nbr, tax_grp_id, tax_item_grp_txt, sls_grp_txt, sales_unit_cd,
    item_height_nbr, item_net_weight_amt, invtry_qty, ordr_qty, non_dlvrd_qty, non_invoiced_dlvrd_qty, sls_qty, sis_qty, unit_cost_price_amt, 
    unit_sls_price_amt, item_default_price_amt, item_online_price_amt, item_list_price_amt, item_cost_amt, total_sls_amt, fixed_misc_amt, imbedded_freight_amt,
    imbedded_freight_cost_amt, overhead_cost_pct, line_dscnt_pct, engineering_hours_nbr, labor_hours_nbr
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SLS_ORDR_LINE
    {% if _load == 1 %}
        where opco_sls_ordr_doc_status_ck not in (select distinct opco_sls_ordr_doc_status_ck from final)
    {% else %}
        where opco_sls_ordr_doc_status_ck not in (select distinct opco_sls_ordr_doc_status_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SLS_ORDR_LINE where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_sls_ordr_doc_status_ck not in (select distinct opco_sls_ordr_doc_status_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SLS_ORDR_LINE where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_sls_ordr_doc_status_ck in (select distinct opco_sls_ordr_doc_status_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SLS_ORDR_LINE where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}