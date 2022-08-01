

with  __dbt__cte__v_ax_opco_sls_ordr_curr as (
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
),  __dbt__cte__v_ax_opco_sls_ordr_type_curr as (
with v_ax_opco_sls_ordr_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr_type

)

select * from v_ax_opco_sls_ordr_type
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
),  __dbt__cte__v_ax_opco_picking_list_curr as (
with v_ax_opco_picking_list as(
    select *,
    rank() over(partition by opco_picking_list_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_picking_list
),
final as(
    select 
    *
    from v_ax_opco_picking_list
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
),  __dbt__cte__v_ax_warehouse_curr as (
with v_ax_warehouse as(
    select *,
    rank() over(partition by warehouse_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_warehouse
),
final as(
    select 
    *
    from v_ax_warehouse
    where rnk = 1 and delete_dtm is null
)

select * from final
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
),  __dbt__cte__v_ax_opco_locn_curr as (
with v_ax_opco_locn as(
    select *,
    rank() over(partition by opco_locn_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_locn
),
final as(
    select 
    *
    from v_ax_opco_locn
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_cust_packing_slip as(
    select 
    current_timestamp as crt_dtm,
    cpj._fivetran_synced as stg_load_dtm,
    case 
        when cpj._fivetran_deleted = true then cpj._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    cpj.dataareaid as opco_id,
    cpj.packingslipid as packing_slip_id,
    oso.opco_sls_ordr_sk,
    osot.opco_sls_ordr_type_sk,
    oc.opco_cust_sk,
    oc1.opco_cust_sk as invoice_cust_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    opl.opco_picking_list_sk,
    os.opco_site_sk,
    ocr.opco_currency_sk as trans_currency_sk,
    opt.opco_pymnt_terms_sk,
    wr.warehouse_sk,
    odt.opco_dlvry_terms_sk,
    odm.opco_dlvry_mode_sk,
    oln.opco_locn_sk as dlvry_locn_sk,
    cpj.ledgervoucher as gl_voucher_nbr,
    cpj.purchaseorder as po_form_nbr,
    cpj.deliverydate as ship_dt,
    cpj.customerref as cust_ref_txt,
    cpj.taxgroup_opi as tax_grp_id,
    case
        when cpj.donotprintinvoice_opi = 0 then 1
        when cpj.donotprintinvoice_opi = 1 then 0
    end as print_invoice_ind,
    cpj.creditreasonid_opi as credit_reason_id,
    cpj.specialinstructions_opi as spcl_instr_txt,
    cpj.registered_opi as registered_Ind,
    cpj.deliveryname as dlvry_cust_nm,
    cpj.invoicingname as invoice_cust_nm,
    cpj.invoicingaddress as invoice_address_txt,
    cpj.loadno_opi as load_nbr,
    cpj.plloadcount_opi as pl_load_cnt,
    cpj.miles_opi as miles_cnt,
    cpj.qty as dlvry_qty,
    cpj.palletqty_opi as pallet_qty,
    cpj.rateperpallet_opi as pallet_rate_amt,
    cpj.volume as dlvry_volume_amt,
    cpj.weight as dlvry_weight_amt,
    cpj.deliverycost_opi as dlvry_cost_amt,
    cpj.packedamount_opi as packing_slip_amt,
    cpj.invoiceid_opi as invoice_nbr,
    cpj.invoicedate_opi as invoice_dt,
    cpj.invoiced_opi as invoice_sent_ind,
    cpj.invoiceamount_opi as invoice_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTPACKINGSLIPJOUR cpj
    left join __dbt__cte__v_ax_opco_sls_ordr_curr oso 
        on cpj.dataareaid = oso.opco_id
        and cpj.salesid = oso. sls_ordr_id
        and oso.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_sls_ordr_type_curr osot 
        on cpj.salestype = osot.src_sls_ordr_type_cd
        and osot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr oc 
        on cpj.dataareaid = oc.opco_id
        and cpj.orderaccount = oc.cust_id
        and oc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr oc1 
        on cpj.dataareaid = oc1.opco_id
        and cpj.invoiceaccount = oc1.cust_id
        and oc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(cpj.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(cpj.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(cpj.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr op 
        on upper(cpj.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol
        on upper(cpj.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_picking_list_curr opl 
        on cpj.dataareaid = opl.opco_id
        and cpj.pickinglistid_opi = opl.picking_list_id
        and opl.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_site_curr os 
        on upper(cpj.inventsiteid_opi) = os.src_site_id
        and os.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr ocr
        on upper(cpj.currencycode_opi) = ocr.src_currency_cd
        and ocr.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_pymnt_terms_curr opt 
        on upper(cpj.payment_opi) = opt.src_pymnt_terms_cd
        and opt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_warehouse_curr wr 
        on cpj.dataareaid = wr.opco_id
        and upper(cpj.inventlocationid) = wr.warehouse_id
        and wr.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dlvry_terms_curr odt 
        on upper(cpj.dlvterm) = odt.src_dlvry_terms_cd
        and odt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dlvry_mode_curr odm 
        on upper(cpj.dlvmode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_locn_curr oln 
         on upper(cpj.deliverystreet) = oln.addr_ln_1_txt
        and upper(cpj.deliverycity) = oln.city_nm
        and upper(cpj.dlvstate) = oln.state_nm
        and upper(cpj.dlvcountryregionid) = oln.country_nm
        and upper(cpj.dlvzipcode) = oln.zip_cd
        and oln.src_sys_nm = 'AX'
    where cpj.dataareaid not in ('044', '045', '047', '999')
),
final as(
    select md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(packing_slip_id as 
    varchar
), '') as 
    varchar
)) as opco_cust_packing_slip_sk,
           md5(cast(coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(packing_slip_id as 
    varchar
), '') || '-' || coalesce(cast(opco_sls_ordr_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sls_ordr_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(invoice_cust_sk as 
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
), '') || '-' || coalesce(cast(opco_picking_list_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_site_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_pymnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(warehouse_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dlvry_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dlvry_mode_sk as 
    varchar
), '') || '-' || coalesce(cast(dlvry_locn_sk as 
    varchar
), '') || '-' || coalesce(cast(gl_voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(po_form_nbr as 
    varchar
), '') || '-' || coalesce(cast(ship_dt as 
    varchar
), '') || '-' || coalesce(cast(cust_ref_txt as 
    varchar
), '') || '-' || coalesce(cast(tax_grp_id as 
    varchar
), '') || '-' || coalesce(cast(print_invoice_ind as 
    varchar
), '') || '-' || coalesce(cast(credit_reason_id as 
    varchar
), '') || '-' || coalesce(cast(spcl_instr_txt as 
    varchar
), '') || '-' || coalesce(cast(registered_Ind as 
    varchar
), '') || '-' || coalesce(cast(dlvry_cust_nm as 
    varchar
), '') || '-' || coalesce(cast(invoice_cust_nm as 
    varchar
), '') || '-' || coalesce(cast(invoice_address_txt as 
    varchar
), '') || '-' || coalesce(cast(load_nbr as 
    varchar
), '') || '-' || coalesce(cast(pl_load_cnt as 
    varchar
), '') || '-' || coalesce(cast(miles_cnt as 
    varchar
), '') || '-' || coalesce(cast(dlvry_qty as 
    varchar
), '') || '-' || coalesce(cast(pallet_qty as 
    varchar
), '') || '-' || coalesce(cast(pallet_rate_amt as 
    varchar
), '') || '-' || coalesce(cast(dlvry_volume_amt as 
    varchar
), '') || '-' || coalesce(cast(dlvry_weight_amt as 
    varchar
), '') || '-' || coalesce(cast(dlvry_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(packing_slip_amt as 
    varchar
), '') || '-' || coalesce(cast(invoice_nbr as 
    varchar
), '') || '-' || coalesce(cast(invoice_dt as 
    varchar
), '') || '-' || coalesce(cast(invoice_sent_ind as 
    varchar
), '') || '-' || coalesce(cast(invoice_amt as 
    varchar
), '') as 
    varchar
)) as opco_cust_packing_slip_ck,
            concat_ws('|', src_sys_nm, opco_id, packing_slip_id) as opco_cust_packing_slip_ak,
            * 
    from ax_opco_cust_packing_slip ocps
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_packing_slip ) 
    
