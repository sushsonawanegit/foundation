

with  __dbt__cte__v_ax_opco_cust_curr as (
with v_ax_opco_cust as(
    select *,
    rank() over(partition by opco_cust_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust
),
final as(
    select 
    *
    from v_ax_opco_cust
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_invtry_trans_type_curr as (
with v_ax_opco_invtry_trans_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_invtry_trans_type
)

select * from v_ax_opco_invtry_trans_type
),  __dbt__cte__v_ax_opco_handling_status_curr as (
with v_ax_opco_handling_status as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_handling_status
)

select * from v_ax_opco_handling_status
),  __dbt__cte__v_ax_opco_dlvry_terms_curr as (
with v_ax_opco_dlvry_terms as(
    select *,
    rank() over(partition by opco_dlvry_terms_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dlvry_terms
),
final as(
    select 
    *
    from v_ax_opco_dlvry_terms
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_dlvry_mode_curr as (
with v_ax_opco_dlvry_mode as(
    select *,
    rank() over(partition by opco_dlvry_mode_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dlvry_mode
),
final as(
    select 
    *
    from v_ax_opco_dlvry_mode
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_site_curr as (
with v_ax_opco_site as(
    select *,
    rank() over(partition by opco_site_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_site
),
final as(
    select 
    *
    from v_ax_opco_site
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_pymnt_terms_curr as (
with v_ax_opco_pymnt_terms as(
    select *,
    rank() over(partition by opco_pymnt_terms_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_pymnt_terms
),
final as(
    select 
    *
    from v_ax_opco_pymnt_terms
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_picking_list as (
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
    from FIVETRAN.AX_PRODREPLICATION1_DBO.WMSPICKINGROUTE wms 
    left join __dbt__cte__v_ax_opco_cust_curr ocu 
        on wms.dataareaid = ocu.opco_id
        and wms.customer = ocu.cust_id
        and ocu.src_sys_nm = 'AX' 
    left join __dbt__cte__v_ax_opco_cust_curr oi 
        on wms.dataareaid = oi.opco_id
        and wms.invoiceaccount_opi = oi.cust_id
        and oi.src_sys_nm = 'AX' 
    left join __dbt__cte__v_ax_opco_invtry_trans_type_curr oit 
        on wms.transtype = oit.invtry_trans_type_cd
        and oit.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_handling_status_curr ohs 
        on wms.expeditionstatus = ohs.handling_status_cd
        and ohs.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dlvry_terms_curr odt 
        on upper(wms.dlvtermid) = odt.src_dlvry_terms_cd
        and odt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dlvry_mode_curr odm 
        on upper(wms.dlvmodeid) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_site_curr os 
        on upper(wms.inventsiteid_opi) = os.src_site_id
        and os.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_pymnt_terms_curr opt 
        on upper(wms.payment_opi) = opt.src_pymnt_terms_cd
        and opt.src_sys_nm = 'AX'
    where wms.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(picking_list_id as 
    varchar
), '') as 
    varchar
)) as opco_picking_list_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(picking_list_id as 
    varchar
), '') || '-' || coalesce(cast(shpmnt_id as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(invoice_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_invtry_trans_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_handling_status_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dlvry_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dlvry_mode_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_site_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_pymnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(src_crt_dt as 
    varchar
), '') || '-' || coalesce(cast(trans_ref_id as 
    varchar
), '') || '-' || coalesce(cast(handling_type_txt as 
    varchar
), '') || '-' || coalesce(cast(start_dtm as 
    varchar
), '') || '-' || coalesce(cast(end_dtm as 
    varchar
), '') || '-' || coalesce(cast(activation_dtm as 
    varchar
), '') || '-' || coalesce(cast(rqstd_ship_dt as 
    varchar
), '') || '-' || coalesce(cast(actl_ship_dt as 
    varchar
), '') || '-' || coalesce(cast(cust_dlvry_dt as 
    varchar
), '') || '-' || coalesce(cast(shpmnt_type_txt as 
    varchar
), '') || '-' || coalesce(cast(dlvry_nm as 
    varchar
), '') || '-' || coalesce(cast(credit_reason_id as 
    varchar
), '') || '-' || coalesce(cast(po_form_nbr as 
    varchar
), '') || '-' || coalesce(cast(tax_grp_id as 
    varchar
), '') || '-' || coalesce(cast(picking_list_updt_ind as 
    varchar
), '') || '-' || coalesce(cast(cust_ref_txt as 
    varchar
), '') || '-' || coalesce(cast(print_invoice_ind as 
    varchar
), '') || '-' || coalesce(cast(item_reserved_ind as 
    varchar
), '') || '-' || coalesce(cast(packing_slip_available_ind as 
    varchar
), '') || '-' || coalesce(cast(dlvry_window_txt as 
    varchar
), '') || '-' || coalesce(cast(spcl_instr_txt as 
    varchar
), '') || '-' || coalesce(cast(srvc_duration_mins_nbr as 
    varchar
), '') || '-' || coalesce(cast(requrmnt_txt as 
    varchar
), '') || '-' || coalesce(cast(picking_list_printed_ind as 
    varchar
), '') || '-' || coalesce(cast(picking_list_printed_after_chg_ind as 
    varchar
), '') || '-' || coalesce(cast(do_not_send_ind as 
    varchar
), '') as 
    varchar
)) as opco_picking_list_Ck,
            concat_ws('|', src_sys_nm, opco_id, picking_list_id) as opco_picking_list_ak, 
            * 
    from ax_opco_picking_list 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_picking_list ) 
    
