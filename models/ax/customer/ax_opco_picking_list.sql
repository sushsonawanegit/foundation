{% set _load = load_type('AX_OPCO_PICKING_LIST') %}

with ax_opco_picking_list as (
    select
    current_timestamp as crt_dtm,
    wms._fivetran_synced as stg_load_dtm,
    case 
        when wms._fivetran_deleted = true then wms._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    wms.dataareaid as opco_id,
    wms.pickingrouteid as picking_list_id,
    wms.shipmentid as shpmnt_id,
    ocu.opco_cust_sk,
    oi.opco_cust_sk as invoice_cust_sk,
    oit.opco_invtry_trans_type_sk,
    ohs.opco_handling_status_sk,
    odt.opco_dlvry_terms_sk,
    odm.opco_dlvry_mode_sk,
    os.opco_site_sk,
    opt.opco_pymnt_terms_sk,
    wms.pickinglistcreatedate_opi as src_crt_dt,
    wms.transrefid as trans_ref_id,
    case 
        when wms.handlingtype = 0 then 'Online'
        when wms.handlingtype = 1 then 'Document'
    end as handling_type_txt,
    wms.startdatetime as start_dtm,
    wms.enddatetime as end_dtm,
    wms.activationdatetime as activation_dtm,
    wms.shippingdaterequested_opi as rqstd_ship_dt,
    wms.dlvdate as actl_ship_dt,
    wms.custdeliverydate_opi as cust_dlvry_dt,
    case
        when wms.shipmenttype = 0 then 'Consolidated Picking'
        when wms.shipmenttype = 1 then 'Order Picking'
    end as shpmnt_type_txt,
    wms.deliveryname as dlvry_nm,
    wms.creditreasonid_opi as credit_reason_id,
    wms.purchorderformnum_opi as po_form_nbr,
    wms.taxgroup_opi as tax_grp_id,
    wms.updated_opi as picking_list_updt_ind,
    wms.customerref_opi as cust_ref_txt,
    case 
        when wms.donotprintinvoice_opi = 0 then 1
        when wms.donotprintinvoice_opi = 1 then 0
    end as print_invoice_ind,
    wms.reserve_opi as item_reserved_ind,
    wms.packingslip_opi as packing_slip_available_ind,
    wms.tmsdlvwindow_opi as dlvry_window_txt,
    wms.tmsequiptype_opi as equipment_type_txt,
    wms.specialinstructions_opi as spcl_instr_txt,
    wms.tmsservicedurationmin_opi as srvc_duration_mins_nbr,
    wms.tmsrequirement_opi as requrmnt_txt,
    wms.printed_opi as picking_list_printed_ind,
    wms.printedafterchange_opi as picking_list_printed_after_chg_ind,
    wms.tmsdonotsend_opi as do_not_send_ind
    from {{ source('AX_DEV', 'WMSPICKINGROUTE') }} wms 
    left join {{ ref('opco_cust')}} ocu 
        on wms.dataareaid = ocu.opco_id
        and wms.customer = ocu.cust_id
        and ocu.src_sys_nm = 'AX' 
    left join {{ ref('opco_cust')}} oi 
        on wms.dataareaid = oi.opco_id
        and wms.invoiceaccount_opi = oi.cust_id
        and oi.src_sys_nm = 'AX' 
    left join {{ ref('opco_invtry_trans_type')}} oit 
        on wms.transtype = oit.invtry_trans_type_cd
        and oit.src_sys_nm = 'AX'
    left join {{ ref('opco_handling_status')}} ohs 
        on wms.expeditionstatus = ohs.handling_status_cd
        and ohs.src_sys_nm = 'AX'
    left join {{ ref('opco_dlvry_terms')}} odt 
        on upper(wms.dlvtermid) = odt.src_dlvry_terms_cd
        and odt.src_sys_nm = 'AX'
    left join {{ ref('opco_dlvry_mode')}} odm 
        on upper(wms.dlvmodeid) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    left join {{ ref('opco_site')}} os 
        on upper(wms.inventsiteid_opi) = os.src_site_id
        and os.src_sys_nm = 'AX'
    left join {{ ref('opco_pymnt_terms')}} opt 
        on upper(wms.payment_opi) = opt.src_pymnt_terms_cd
        and opt.src_sys_nm = 'AX'
    where wms.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'picking_list_id']) }} as opco_picking_list_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'picking_list_id', 'shpmnt_id', 'opco_cust_sk', 'invoice_cust_sk', 'opco_invtry_trans_type_sk', 'opco_handling_status_sk', 'opco_dlvry_terms_sk', 'opco_dlvry_mode_sk', 'opco_site_sk', 'opco_pymnt_terms_sk', 'src_crt_dt', 'trans_ref_id', 'handling_type_txt', 'start_dtm', 'end_dtm', 'activation_dtm', 'rqstd_ship_dt', 'actl_ship_dt', 'cust_dlvry_dt', 'shpmnt_type_txt', 'dlvry_nm', 'credit_reason_id', 'po_form_nbr', 'tax_grp_id', 'picking_list_updt_ind', 'cust_ref_txt', 'print_invoice_ind', 'item_reserved_ind', 'packing_slip_available_ind', 'dlvry_window_txt', 'spcl_instr_txt', 'srvc_duration_mins_nbr', 'requrmnt_txt', 'picking_list_printed_ind', 'picking_list_printed_after_chg_ind', 'do_not_send_ind']) }} as opco_picking_list_Ck,
            concat_ws('|', src_sys_nm, opco_id, picking_list_id) as opco_picking_list_ak, 
            * 
    from ax_opco_picking_list 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_PICKING_LIST') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PICKING_LIST') and _load != 3%}
    union
    select
    opco_picking_list_sk, opco_picking_list_ck, opco_picking_list_ak, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, opco_id, picking_list_id, shpmnt_id, opco_cust_sk, invoice_cust_sk, opco_invtry_trans_type_sk, opco_handling_status_sk, opco_dlvry_terms_sk,
    opco_dlvry_mode_sk, opco_site_sk, opco_pymnt_terms_sk, src_crt_dt, trans_ref_id, handling_type_txt, start_dtm, end_dtm, activation_dtm, rqstd_ship_dt,
    actl_ship_dt, cust_dlvry_dt, shpmnt_type_txt, dlvry_nm, credit_reason_id, po_form_nbr, tax_grp_id, picking_list_updt_ind, cust_ref_txt, print_invoice_ind,
    item_reserved_ind, packing_slip_available_ind, dlvry_window_txt, spcl_instr_txt, srvc_duration_mins_nbr, requrmnt_txt, picking_list_printed_ind, 
    picking_list_printed_after_chg_ind, do_not_send_ind
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PICKING_LIST
    {% if _load == 1 %}
        where opco_picking_list_ck not in (select distinct opco_picking_list_ck from final)
    {% else %}
        where opco_picking_list_ck not in (select distinct opco_picking_list_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PICKING_LIST where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_picking_list_ck not in (select distinct opco_picking_list_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PICKING_LIST where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_picking_list_ck in (select distinct opco_picking_list_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PICKING_LIST where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  