

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
),  __dbt__cte__v_ax_opco_cust_packing_slip_curr as (
with v_ax_opco_cust_packing_slip as(
    select *,
    rank() over(partition by opco_cust_packing_slip_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_packing_slip
),
final as(
    select 
    *
    from v_ax_opco_cust_packing_slip
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
),  __dbt__cte__v_ax_opco_sls_ordr_type_curr as (
with v_ax_opco_sls_ordr_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr_type

)

select * from v_ax_opco_sls_ordr_type
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
),  __dbt__cte__v_ax_opco_cust_grp_curr as (
with v_ax_opco_cust_grp as(
    select *,
    rank() over(partition by opco_cust_grp_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_grp
),
final as(
    select 
    * 
    from v_ax_opco_cust_grp
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
),  __dbt__cte__v_ax_opco_cash_dscnt_terms_curr as (
with v_ax_opco_cash_dscnt_terms as(
    select *,
    rank() over(partition by opco_cash_dscnt_terms_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cash_dscnt_terms
),
final as(
    select 
    *
    from v_ax_opco_cash_dscnt_terms
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
),ax_opco_cust_invoice as (
    select 
    current_timestamp as crt_dtm,
    cij._fivetran_synced as stg_load_dtm,
    case 
        when cij._fivetran_deleted = true then cij._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    cij.recid as src_key_txt,
    opco.opco_sk,
    oc.opco_cust_sk,
    oso.opco_sls_ordr_sk,
    opl.opco_picking_list_sk,
    ocps.opco_cust_packing_slip_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    osot.opco_sls_ordr_type_sk,
    oc1.opco_cust_sk as invoice_cust_sk,
    ocr.opco_currency_sk as trans_currency_sk,
    ocg.opco_cust_grp_sk,
    opt.opco_pymnt_terms_sk,
    ocdt.opco_cash_dscnt_terms_sk,
    odt.opco_dlvry_terms_sk,
    odm.opco_dlvry_mode_sk,
    oln.opco_locn_sk as dlvry_locn_sk,
    oln1.opco_locn_sk as invoice_locn_sk,
    cij.invoiceid as invoice_nbr,
    case
        when cij.refnum = 0 then 'Sales Order'
        when cij.refnum = 1 then 'Project'
        when cij.refnum = 2 then 'Customer'
        when cij.refnum = 3 then 'Interest'
    end as trans_ref_txt,
    cij.invoicedate as invoice_dt,
    cij.duedate as due_dt,
    cij.cashdiscdate as cash_dscnt_dt,
    cij.ledgervoucher as gl_voucher_nbr,
    cij.documentnum as document_nbr,
    cij.documentdate as document_dt,
    cij.deliveryname as dlvry_cust_nm,
    cij.invoicingname as invoice_cust_nm,
    cij.salesgroup_opi as sls_grp_txt,
    cij.incltax as sls_tax_included_ind,
    cij.updated as updt_after_printing_ind,
    cij.log as specific_action_txt,
    cij.dispatchlocation_opi as dispatch_locn_txt,
    cij.paymentsched as pymnt_sched_cd,
    cij.del_leadcode_opi as lead_cd,
    cij.purchaseorder as po_form_nbr,
    cij.commissiongroup_opi as commsn_grp_id,
    cij.creditreason_opi as credit_reason_id,
    cij.custendcustomerid_opi as end_cust_id,
    cij.taxgroup as tax_grp_id,
    cij.qty as invoice_qty,
    cij.volume as invoice_volume_amt,
    cij.weight as invoice_weight_amt,
    cij.salesbalance as trans_currency_balance_sls_amt,
    cij.salesbalancemst as opco_currency_balance_sls_amt,
    cij.invoiceamount as trans_currency_invoice_amt,
    cij.invoiceamountmst as opco_currency_invoice_amt,
    cij.sumlinedisc as trans_currency_line_dscnt_amt,
    cij.sumlinediscmst as opco_currency_line_dscnt_amt,
    cij.cashdisc as trans_currency_cash_dscnt_amt,
    cij.sumtax as trans_currency_sls_tax_amt,
    cij.sumtaxmst as opco_currency_sls_tax_amt,
    cij.summarkup as trans_currency_misc_chrgs_amt,
    cij.summarkupmst as opco_currenty_misc_chrgs_amt,
    cij.deliverycost_opi as dlvry_cost_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTINVOICEJOUR cij 
    left join __dbt__cte__v_ax_opco_curr opco 
        on cij.dataareaid = opco.opco_id 
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr oc 
        on cij.dataareaid = oc.opco_id
        and cij.orderaccount = oc.cust_id
        and oc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_sls_ordr_curr oso 
        on cij.dataareaid = oso.opco_id
        and cij.salesid = oso.sls_ordr_id
        and oso.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_picking_list_curr opl 
        on cij.dataareaid = opl.opco_id
        and cij.pickinglistid_opi = opl.picking_list_id
        and opl.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_packing_slip_curr ocps 
        on cij.dataareaid = ocps.opco_id
        and cij.packingslipid_opi = ocps.packing_slip_id
        and ocps.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(cij.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(cij.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(cij.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr op 
        on upper(cij.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol
        on upper(cij.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_sls_ordr_type_curr osot 
        on cij.salestype = osot.src_sls_ordr_type_cd
        and osot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr oc1   
        on cij.dataareaid = oc1.opco_id
        and cij.invoiceaccount = oc1.cust_id
        and oc1.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr ocr 
        on upper(cij.currencycode) = ocr.src_currency_cd
        and ocr.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_grp_curr ocg 
        on upper(cij.custgroup) = ocg.src_cust_grp_cd
        and ocg.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_pymnt_terms_curr opt 
        on upper(cij.payment) = opt.src_pymnt_terms_cd
        and opt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cash_dscnt_terms_curr ocdt 
        on upper(cij.cashdisccode) = ocdt.src_cash_dscnt_terms_cd
        and ocdt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dlvry_terms_curr odt 
        on upper(cij.dlvterm) = odt.src_dlvry_terms_cd
        and odt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dlvry_mode_curr odm 
        on upper(cij.dlvmode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_locn_curr oln 
         on upper(cij.deliverystreet) = oln.addr_ln_1_txt
        and upper(cij.deliverycity) = oln.city_nm
        and upper(cij.dlvstate) = oln.state_nm
        and upper(cij.dlvcountryregionid) = oln.country_nm
        and upper(cij.dlvzipcode) = oln.zip_cd
        and oln.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_locn_curr oln1 
         on upper(cij.invoicestreet) = oln1.addr_ln_1_txt
        and upper(cij.invoicecity) = oln1.city_nm
        and upper(cij.invstate) = oln1.state_nm
        and upper(cij.invcountryregionid) = oln1.country_nm
        and upper(cij.invzipcode) = oln1.zip_cd
        and oln1.src_sys_nm = 'AX'
    where cij.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') as 
    varchar
)) as opco_cust_invoice_sk,
           md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sls_ordr_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_picking_list_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_packing_slip_sk as 
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
), '') || '-' || coalesce(cast(opco_sls_ordr_type_sk as 
    varchar
), '') || '-' || coalesce(cast(invoice_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_grp_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_pymnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cash_dscnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dlvry_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dlvry_mode_sk as 
    varchar
), '') || '-' || coalesce(cast(dlvry_locn_sk as 
    varchar
), '') || '-' || coalesce(cast(invoice_locn_sk as 
    varchar
), '') || '-' || coalesce(cast(invoice_nbr as 
    varchar
), '') || '-' || coalesce(cast(trans_ref_txt as 
    varchar
), '') || '-' || coalesce(cast(invoice_dt as 
    varchar
), '') || '-' || coalesce(cast(due_dt as 
    varchar
), '') || '-' || coalesce(cast(cash_dscnt_dt as 
    varchar
), '') || '-' || coalesce(cast(gl_voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(document_nbr as 
    varchar
), '') || '-' || coalesce(cast(document_dt as 
    varchar
), '') || '-' || coalesce(cast(dlvry_cust_nm as 
    varchar
), '') || '-' || coalesce(cast(invoice_cust_nm as 
    varchar
), '') || '-' || coalesce(cast(sls_grp_txt as 
    varchar
), '') || '-' || coalesce(cast(sls_tax_included_ind as 
    varchar
), '') || '-' || coalesce(cast(updt_after_printing_ind as 
    varchar
), '') || '-' || coalesce(cast(specific_action_txt as 
    varchar
), '') || '-' || coalesce(cast(dispatch_locn_txt as 
    varchar
), '') || '-' || coalesce(cast(pymnt_sched_cd as 
    varchar
), '') || '-' || coalesce(cast(lead_cd as 
    varchar
), '') || '-' || coalesce(cast(po_form_nbr as 
    varchar
), '') || '-' || coalesce(cast(commsn_grp_id as 
    varchar
), '') || '-' || coalesce(cast(credit_reason_id as 
    varchar
), '') || '-' || coalesce(cast(end_cust_id as 
    varchar
), '') || '-' || coalesce(cast(tax_grp_id as 
    varchar
), '') || '-' || coalesce(cast(invoice_qty as 
    varchar
), '') || '-' || coalesce(cast(invoice_volume_amt as 
    varchar
), '') || '-' || coalesce(cast(invoice_weight_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_balance_sls_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_balance_sls_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_invoice_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_invoice_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_line_dscnt_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_line_dscnt_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_cash_dscnt_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sls_tax_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_sls_tax_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_misc_chrgs_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currenty_misc_chrgs_amt as 
    varchar
), '') || '-' || coalesce(cast(dlvry_cost_amt as 
    varchar
), '') as 
    varchar
)) as opco_cust_invoice_ck,
            concat_ws('|', src_sys_nm, src_key_txt) as opco_cust_invoice_ak, 
            *
    from ax_opco_cust_invoice 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_invoice ) 
    
