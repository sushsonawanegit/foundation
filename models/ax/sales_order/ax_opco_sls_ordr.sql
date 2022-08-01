{% set _load = load_type('AX_OPCO_SLS_ORDR') %}

with ax_opco_sls_ordr as (
    select
    current_timestamp as crt_dtm,
    st._fivetran_synced as stg_load_dtm,
    case
        when st._fivetran_deleted = true then st._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    st.dataareaid as opco_id,
    st.salesid as sls_ordr_id,
    st.salesname as sls_ordr_nm,
    st.orderdate_opi as sls_ordr_dt,
    st.deliverydate as ship_dt,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    oip.opco_purpose_sk,
    ol.opco_lob_sk,
    oc.opco_cust_sk,
    oss.opco_trans_status_sk,
    ocg.opco_cust_grp_sk,
    os.opco_site_sk,
    odt.opco_dlvry_terms_sk,
    odm.opco_dlvry_mode_sk,
    olc.opco_locn_sk as dlvry_loc_sk,
    wr.warehouse_sk,
    st.salesgroup as sls_grp_nm,
    st.taxgroup as tax_grp_id,
    st.quotationid as quotation_id,
    st.purchorderformnum as cust_po_nbr,
    ocu.opco_currency_sk as trans_currency_sk,
    opt.opco_pymnt_terms_sk,
    opm.opco_pymnt_mode_sk,
    ocd.opco_cash_dscnt_terms_sk,
    st.cashdiscpercent as cash_dscnt_pct,
    st.createddatetime as sls_ordr_crt_dtm,
    st.modifieddatetime as sls_ordr_modified_dtm,
    st.createdby as sls_ordr_crt_by_nm,
    st.modifiedby as sls_ordr_modified_by_nm,
    st.onhold_opi as on_hold_ind,
    st.onholdby_opi as on_hold_by_nm,
    st.onholdreason_opi as on_hold_rsn_desc,
    st.discpercent as dscnt_pct,
    st.custendcustomerid_opi as end_cust_nm,
    st.customerref as cust_ref_txt,
    st.deliveryname as dlvry_cust_nm,
    oso.opco_sls_ordr_doc_status_sk,
    ocust.opco_cust_sk as invoice_cust_sk,
    st.linedisc as line_dscnt_cd,
    st.phaseid_opi as sls_ordr_phase_cd,
    st.pricegroupid as trade_grp_cd,
    -- pdg.name as trade_grp_nm,
    '' as trade_grp_nm,
    -- pdg.module as trade_grp_module_txt,
    '' as trade_grp_module_txt,
    -- pdg.type as trade_grp_type_txt,
    '' as trade_grp_type_txt,
    st.receiptdateconfirmed as rcpt_cnfrm_dt,
    st.receiptdaterequested as rcpt_rqst_dt,
    st.salestaker as sls_taker_nm,
    osot.opco_sls_ordr_type_sk,
    st.shippingdateconfirmed as ship_cnfrm_dt,
    st.shippingdaterequested as ship_rqst_dt,
    st.titanjobnum_opi as titan_job_nbr,
    st.creatededi_ep_opi as online_ordr_ind,
    st.specialinstructions_opi as spcl_instr_txt,
    st.taxexemptnumber_opi as tax_exempt_nbr,
    st.commissiongroup as commsn_grp_id,
    ccg.name as commsn_grp_nm,
    st.deadline as last_acceptance_dt,
    st.fixedduedate as fixed_due_dt,
    st.backlogcost_opi as backlog_cost_amt,
    st.backlogsales_opi as backlog_sls_amt,
    st.biddate_opi as bid_dt,
    case
        when st.donotprintinvoice_opi = 0 then 1
        when st.donotprintinvoice_opi = 1 then 0
    end as print_invoice_ind,
    st.del_leadcode_opi as sls_lead_cd,
    st.namealias_opi as cust_alt_nm,
    st.dispatchlocation_opi as dsptch_locn_nm,
    st.tmsequiptype_opi as equipment_type_txt
    from {{source('AX_DEV', 'SALESTABLE')}} st 
    -- left join {{ source('AX_DEV', 'PRICEDISCGROUP') }} pdg 
    --     on st.dataareaid = pdg.dataareaid
    --     and st.pricegroupid = pdg.groupid
    --     and st.dataareaid not in {{ var('excluded_ax_companies')}}
    --     and pdg.dataareaid not in {{ var('excluded_ax_companies')}}
    --     and pdg._fivetran_deleted = false
    left join {{ source('AX_DEV', 'COMMISSIONCUSTOMERGROUP') }} ccg
        on st.dataareaid = ccg.dataareaid
        and st.commissiongroup = ccg.groupid
        and st.dataareaid not in {{ var('excluded_ax_companies')}}
        and ccg.dataareaid not in {{ var('excluded_ax_companies')}}
        and ccg._fivetran_deleted = false
    left join {{ref('ax_opco_cost_center_curr')}} occ 
        on upper(st.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ref('ax_opco_dept_curr')}} od 
        on upper(st.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ref('ax_opco_type_curr')}} ot 
        on upper(st.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ref('ax_opco_purpose_curr')}} oip 
        on upper(st.dimension4_) = oip.src_purpose_cd
        and oip.src_sys_nm = 'AX'
    left join {{ref('ax_opco_lob_curr')}} ol
        on upper(st.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ref('ax_opco_cust_curr')}} oc 
        on st.dataareaid = oc.opco_id
        and st.custaccount = oc.cust_id
        and oc.src_sys_nm = 'AX'
    left join {{ref('ax_opco_trans_status_curr')}} oss 
        on st.salesstatus = oss.src_trans_status_cd
        and oss.src_sys_nm = 'AX'
    left join {{ref('ax_opco_cust_grp_curr')}} ocg 
        on upper(st.custgroup) = ocg.src_cust_grp_cd
        and ocg.src_sys_nm = 'AX'
    left join {{ref('ax_opco_site_curr')}} os 
        on upper(st.inventsiteid) = os.src_site_id
        and os.src_sys_nm = 'AX'
    left join {{ref('ax_opco_currency_curr')}} ocu 
        on upper(st.currencycode) = ocu.src_currency_cd
        and ocu.src_sys_nm = 'AX'
    left join {{ref('ax_opco_pymnt_terms_curr')}} opt 
        on upper(st.payment) = opt.src_pymnt_terms_cd
        and opt.src_sys_nm = 'AX'
    left join {{ref('ax_opco_pymnt_mode_curr')}} opm 
        on upper(st.paymmode) = opm.src_pymnt_mode_cd
        and opm.src_sys_nm = 'AX'
    left join {{ref('ax_opco_cash_dscnt_terms_curr')}} ocd 
        on upper(st.cashdisc) = ocd.src_cash_dscnt_terms_cd
        and ocd.src_sys_nm = 'AX'
    left join {{ref('ax_opco_sls_ordr_doc_status_curr')}} oso 
        on st.documentstatus = oso.src_sls_ordr_doc_status_cd
        and oso.src_sys_nm = 'AX'
    left join {{ref('ax_opco_sls_ordr_type_curr')}} osot
        on st.salestype = osot.src_sls_ordr_type_cd
        and osot.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_dlvry_terms_curr')}} odt 
        on upper(st.dlvterm) = odt.src_dlvry_terms_cd
        and odt.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_dlvry_mode_curr')}} odm 
        on upper(st.dlvmode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_locn_curr')}} olc
        on st.deliverystreet = olc.addr_ln_1_txt
        and st.deliverycity = olc.city_nm
        and st.deliverystate = olc.state_nm
        and st.deliverycountryregionid = olc.country_nm
        and st.deliveryzipcode = olc.zip_cd
    left join {{ ref('ax_warehouse_curr')}} wr 
        on upper(st.inventlocationid) = wr.warehouse_id
        and st.dataareaid = wr.opco_id
        and wr.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_cust_curr')}} ocust
        on st.dataareaid = ocust.opco_id
        and st.invoiceaccount = ocust.cust_id
        and ocust.src_sys_nm = 'AX'
    where st.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select 
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'sls_ordr_id']) }} as opco_sls_ordr_sk,
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'sls_ordr_id', 'sls_ordr_nm', 'sls_ordr_dt', 'ship_dt', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_cust_sk', 'opco_trans_status_sk', 'opco_cust_grp_sk', 'opco_site_sk', 'opco_dlvry_terms_sk', 'opco_dlvry_mode_sk', 'dlvry_loc_sk', 'warehouse_sk', 'sls_grp_nm', 'tax_grp_id', 'quotation_id', 'cust_po_nbr', 'trans_currency_sk', 'opco_pymnt_terms_sk', 'opco_pymnt_mode_sk', 'opco_cash_dscnt_terms_sk', 'cash_dscnt_pct', 'sls_ordr_crt_dtm', 'sls_ordr_modified_dtm', 'sls_ordr_crt_by_nm', 'sls_ordr_modified_by_nm', 'on_hold_ind', 'on_hold_by_nm', 'on_hold_rsn_desc', 'dscnt_pct', 'end_cust_nm', 'cust_ref_txt', 'dlvry_cust_nm', 'opco_sls_ordr_doc_status_sk', 'invoice_cust_sk', 'line_dscnt_cd', 'sls_ordr_phase_cd', 'trade_grp_cd', 'trade_grp_nm', 'trade_grp_module_txt', 'trade_grp_type_txt', 'rcpt_cnfrm_dt', 'rcpt_rqst_dt', 'sls_taker_nm', 'opco_sls_ordr_type_sk', 'ship_cnfrm_dt', 'ship_rqst_dt', 'titan_job_nbr', 'online_ordr_ind', 'spcl_instr_txt', 'tax_exempt_nbr', 'commsn_grp_id', 'commsn_grp_nm', 'last_acceptance_dt', 'fixed_due_dt', 'backlog_cost_amt', 'backlog_sls_amt', 'bid_dt', 'print_invoice_ind', 'sls_lead_cd', 'cust_alt_nm', 'dsptch_locn_nm', 'equipment_type_txt']) }} as opco_sls_ordr_ck, 
        concat_ws('|', src_sys_nm, opco_id, sls_ordr_id) as opco_sls_ordr_ak, 
        * 
    from ax_opco_sls_ordr 
)

select * from final   
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_SLS_ORDR') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_SLS_ORDR') and _load != 3%}
    union
    select
    opco_sls_ordr_sk, opco_sls_ordr_ck, opco_sls_ordr_ak, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, opco_id, sls_ordr_id, sls_ordr_nm, sls_ordr_dt, ship_dt, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk,
    opco_lob_sk, opco_cust_sk, opco_trans_status_sk, opco_cust_grp_sk, opco_site_sk, opco_dlvry_terms_sk, opco_dlvry_mode_sk, dlvry_loc_sk,
    warehouse_sk, sls_grp_nm, tax_grp_id, quotation_id, cust_po_nbr, trans_currency_sk, opco_pymnt_terms_sk, opco_pymnt_mode_sk, opco_cash_dscnt_terms_sk,
    cash_dscnt_pct, sls_ordr_crt_dtm, sls_ordr_modified_dtm, sls_ordr_crt_by_nm, sls_ordr_modified_by_nm, on_hold_ind, on_hold_by_nm, on_hold_rsn_desc, 
    dscnt_pct, end_cust_nm, cust_ref_txt, dlvry_cust_nm, opco_sls_ordr_doc_status_sk, invoice_cust_sk, line_dscnt_cd, sls_ordr_phase_cd, trade_grp_cd,
    trade_grp_nm, trade_grp_module_txt, trade_grp_type_txt, rcpt_cnfrm_dt, rcpt_rqst_dt, sls_taker_nm, opco_sls_ordr_type_sk, ship_cnfrm_dt, 
    ship_rqst_dt, titan_job_nbr, online_ordr_ind, spcl_instr_txt, tax_exempt_nbr, commsn_grp_id, commsn_grp_nm, last_acceptance_dt, fixed_due_dt, 
    backlog_cost_amt, backlog_sls_amt, bid_dt, print_invoice_ind, sls_lead_cd, cust_alt_nm, dsptch_locn_nm, equipment_type_txt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SLS_ORDR
    {% if _load == 1 %}
        where opco_sls_ordr_ck not in (select distinct opco_sls_ordr_ck from final)
    {% else %}
        where opco_sls_ordr_ck not in (select distinct opco_sls_ordr_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SLS_ORDR where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_sls_ordr_ck not in (select distinct opco_sls_ordr_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SLS_ORDR where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_sls_ordr_ck in (select distinct opco_sls_ordr_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SLS_ORDR where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}