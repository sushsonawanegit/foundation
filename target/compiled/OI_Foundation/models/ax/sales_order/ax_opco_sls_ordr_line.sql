

with  __dbt__cte__v_ax_opco_curr as (
with v_ax_opco as(
    select *,
    rank() over(partition by opco_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco
),
final as(
    select 
    *
    from v_ax_opco
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_sls_ordr_curr as (
with v_ax_opco_sls_ordr as(
    select *,
    rank() over(partition by opco_sls_ordr_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr
),
final as(
    select 
    *
    from v_ax_opco_sls_ordr
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_item_curr as (
with v_ax_opco_item as(
    select *,
    rank() over(partition by opco_item_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item
),
final as(
    select 
    *
    from v_ax_opco_item
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_cust_curr as (
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
),  __dbt__cte__v_ax_opco_currency_curr as (
with v_ax_opco_currency as(
    select *,
    rank() over(partition by opco_currency_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_currency
),
final as(
    select 
    *
    from v_ax_opco_currency
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_uom_curr as (
with v_ax_opco_uom as(
    select *,
    rank() over(partition by opco_uom_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_uom
),
final as(
    select 
    *
    from v_ax_opco_uom
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_cost_center_curr as (
with v_ax_opco_cost_center as(
    select *,
    rank() over(partition by opco_cost_center_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cost_center
),
final as(
    select 
    *
    from v_ax_opco_cost_center
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_dept_curr as (
with v_ax_opco_dept as(
    select *,
    rank() over(partition by opco_dept_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dept
),
final as(
    select 
    *
    from v_ax_opco_dept
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_type_curr as (
with v_ax_opco_type as(
    select *,
    rank() over(partition by opco_type_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_type
),
final as(
    select 
    *
    from v_ax_opco_type
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_purpose_curr as (
with v_ax_opco_purpose as(
    select *,
    rank() over(partition by opco_purpose_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_purpose
),
final as(
    select 
    *
    from v_ax_opco_purpose
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_lob_curr as (
with v_ax_opco_lob as(
    select *,
    rank() over(partition by opco_lob_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_lob
),
final as(
    select 
    *
    from v_ax_opco_lob
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_assctn_curr as (
with v_ax_opco_assctn as(
    select *,
    rank() over(partition by opco_assctn_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_assctn
),
final as(
    select 
    *
    from v_ax_opco_assctn
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_sls_ordr_type_curr as (
with v_ax_opco_sls_ordr_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr_type

)

select * from v_ax_opco_sls_ordr_type
),  __dbt__cte__v_ax_opco_trans_status_curr as (
with v_ax_opco_trans_status as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_trans_status
)

select * from v_ax_opco_trans_status
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
),ax_opco_sls_ordr_line as (
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
    from FIVETRAN.AX_PRODREPLICATION1_DBO.SALESLINE sl 
    left join __dbt__cte__v_ax_opco_curr opco 
        on sl.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_sls_ordr_curr oso 
        on sl.dataareaid = oso.opco_id
        and sl.salesid = oso.sls_ordr_id
        and oso.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_item_curr oi 
        on sl.dataareaid = oi.opco_id
        and sl.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr oc 
        on sl.dataareaid = oc.opco_id
        and sl.custaccount = oc.cust_id
        and oc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr ocu 
        on upper(sl.currencycode) = ocu.src_currency_cd
        and ocu.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_uom_curr ou 
        on upper(sl.salesunit) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(sl.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(sl.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(sl.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr oip 
        on upper(sl.dimension4_) = oip.src_purpose_cd
        and oip.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol 
        on upper(sl.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_assctn_curr oa 
        on sl.inventdimid = oa.src_assctn_cd
        and sl.dataareaid = oa.opco_id
        and oa.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_sls_ordr_type_curr osot 
        on sl.salestype = osot.src_sls_ordr_type_cd
        and osot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_trans_status_curr osos
        on sl.salesstatus = osos.src_trans_status_cd
        and osos.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dlvry_mode_curr odm 
        on upper(sl.dlvmode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    where sl.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select distinct
           md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_id as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_line_nbr as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_id as 
    varchar
), '') as 
    varchar
)) as opco_sls_ordr_line_sk,
           md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_id as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_line_nbr as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_id as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sls_ordr_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cost_center_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dept_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_purpose_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_lob_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_assctn_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_trans_status_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sls_ordr_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dlvry_mode_sk as 
    varchar
), '') || '-' || coalesce(cast(line_blocked_ind as 
    varchar
), '') || '-' || coalesce(cast(line_dlvry_complete_ind as 
    varchar
), '') || '-' || coalesce(cast(item_nm as 
    varchar
), '') || '-' || coalesce(cast(dlvry_type_txt as 
    varchar
), '') || '-' || coalesce(cast(po_form_nbr as 
    varchar
), '') || '-' || coalesce(cast(drawing_locn_txt as 
    varchar
), '') || '-' || coalesce(cast(item_bom_id as 
    varchar
), '') || '-' || coalesce(cast(dlvry_date_contrl_txt as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_line_crt_dtm as 
    varchar
), '') || '-' || coalesce(cast(rqst_rcpt_dt as 
    varchar
), '') || '-' || coalesce(cast(cnfrm_rcpt_dt as 
    varchar
), '') || '-' || coalesce(cast(rqst_ship_dt as 
    varchar
), '') || '-' || coalesce(cast(cnfrm_ship_dt as 
    varchar
), '') || '-' || coalesce(cast(cnfrm_dlvry_dt as 
    varchar
), '') || '-' || coalesce(cast(grp_nm as 
    varchar
), '') || '-' || coalesce(cast(trans_ref_id as 
    varchar
), '') || '-' || coalesce(cast(mrkt_cd as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_dtl_id as 
    varchar
), '') || '-' || coalesce(cast(phase_id as 
    varchar
), '') || '-' || coalesce(cast(priority_cd as 
    varchar
), '') || '-' || coalesce(cast(risk_ind as 
    varchar
), '') || '-' || coalesce(cast(spcl_item_ind as 
    varchar
), '') || '-' || coalesce(cast(spcl_in_spcl_item_ind as 
    varchar
), '') || '-' || coalesce(cast(spcl_item_id as 
    varchar
), '') || '-' || coalesce(cast(titan_job_nbr as 
    varchar
), '') || '-' || coalesce(cast(tax_grp_id as 
    varchar
), '') || '-' || coalesce(cast(tax_item_grp_txt as 
    varchar
), '') || '-' || coalesce(cast(sls_grp_txt as 
    varchar
), '') || '-' || coalesce(cast(sales_unit_cd as 
    varchar
), '') || '-' || coalesce(cast(item_height_nbr as 
    varchar
), '') || '-' || coalesce(cast(item_net_weight_amt as 
    varchar
), '') || '-' || coalesce(cast(invtry_qty as 
    varchar
), '') || '-' || coalesce(cast(ordr_qty as 
    varchar
), '') || '-' || coalesce(cast(non_dlvrd_qty as 
    varchar
), '') || '-' || coalesce(cast(non_invoiced_dlvrd_qty as 
    varchar
), '') || '-' || coalesce(cast(sls_qty as 
    varchar
), '') || '-' || coalesce(cast(sis_qty as 
    varchar
), '') || '-' || coalesce(cast(unit_cost_price_amt as 
    varchar
), '') || '-' || coalesce(cast(unit_sls_price_amt as 
    varchar
), '') || '-' || coalesce(cast(item_default_price_amt as 
    varchar
), '') || '-' || coalesce(cast(item_online_price_amt as 
    varchar
), '') || '-' || coalesce(cast(item_list_price_amt as 
    varchar
), '') || '-' || coalesce(cast(item_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(total_sls_amt as 
    varchar
), '') || '-' || coalesce(cast(fixed_misc_amt as 
    varchar
), '') || '-' || coalesce(cast(imbedded_freight_amt as 
    varchar
), '') || '-' || coalesce(cast(imbedded_freight_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(overhead_cost_pct as 
    varchar
), '') || '-' || coalesce(cast(line_dscnt_pct as 
    varchar
), '') || '-' || coalesce(cast(engineering_hours_nbr as 
    varchar
), '') || '-' || coalesce(cast(labor_hours_nbr as 
    varchar
), '') as 
    varchar
)) as opco_sls_ordr_line_ck,
            concat_ws('|', src_sys_nm, opco_id, sls_ordr_id, sls_ordr_line_nbr, invtry_trans_id) as opco_sls_ordr_line_ak, 
            * 
    from ax_opco_sls_ordr_line 
    
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr_line ) 
    
