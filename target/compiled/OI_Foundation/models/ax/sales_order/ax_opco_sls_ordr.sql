

with  __dbt__cte__v_ax_opco_cost_center_curr as (
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
),  __dbt__cte__v_ax_opco_trans_status_curr as (
with v_ax_opco_trans_status as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_trans_status
)

select * from v_ax_opco_trans_status
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
),  __dbt__cte__v_ax_opco_pymnt_mode_curr as (
with v_ax_opco_pymnt_mode as(
    select *,
    rank() over(partition by opco_pymnt_mode_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_pymnt_mode
),
final as(
    select 
    *
    from v_ax_opco_pymnt_mode
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
),  __dbt__cte__v_ax_opco_sls_ordr_doc_status_curr as (
with v_ax_opco_sls_ordr_doc_status as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr_doc_status
)

select * from v_ax_opco_sls_ordr_doc_status
),  __dbt__cte__v_ax_opco_sls_ordr_type_curr as (
with v_ax_opco_sls_ordr_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr_type

)

select * from v_ax_opco_sls_ordr_type
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
),ax_opco_sls_ordr as (
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
    from FIVETRAN.AX_PRODREPLICATION1_DBO.SALESTABLE st 
    -- left join FIVETRAN.AX_PRODREPLICATION1_DBO.PRICEDISCGROUP pdg 
    --     on st.dataareaid = pdg.dataareaid
    --     and st.pricegroupid = pdg.groupid
    --     and st.dataareaid not in ('044', '045', '047', '999')
    --     and pdg.dataareaid not in ('044', '045', '047', '999')
    --     and pdg._fivetran_deleted = false
    left join FIVETRAN.AX_PRODREPLICATION1_DBO.COMMISSIONCUSTOMERGROUP ccg
        on st.dataareaid = ccg.dataareaid
        and st.commissiongroup = ccg.groupid
        and st.dataareaid not in ('044', '045', '047', '999')
        and ccg.dataareaid not in ('044', '045', '047', '999')
        and ccg._fivetran_deleted = false
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(st.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(st.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(st.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr oip 
        on upper(st.dimension4_) = oip.src_purpose_cd
        and oip.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol
        on upper(st.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr oc 
        on st.dataareaid = oc.opco_id
        and st.custaccount = oc.cust_id
        and oc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_trans_status_curr oss 
        on st.salesstatus = oss.src_trans_status_cd
        and oss.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_grp_curr ocg 
        on upper(st.custgroup) = ocg.src_cust_grp_cd
        and ocg.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_site_curr os 
        on upper(st.inventsiteid) = os.src_site_id
        and os.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr ocu 
        on upper(st.currencycode) = ocu.src_currency_cd
        and ocu.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_pymnt_terms_curr opt 
        on upper(st.payment) = opt.src_pymnt_terms_cd
        and opt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_pymnt_mode_curr opm 
        on upper(st.paymmode) = opm.src_pymnt_mode_cd
        and opm.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cash_dscnt_terms_curr ocd 
        on upper(st.cashdisc) = ocd.src_cash_dscnt_terms_cd
        and ocd.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_sls_ordr_doc_status_curr oso 
        on st.documentstatus = oso.src_sls_ordr_doc_status_cd
        and oso.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_sls_ordr_type_curr osot
        on st.salestype = osot.src_sls_ordr_type_cd
        and osot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dlvry_terms_curr odt 
        on upper(st.dlvterm) = odt.src_dlvry_terms_cd
        and odt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dlvry_mode_curr odm 
        on upper(st.dlvmode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_locn_curr olc
        on st.deliverystreet = olc.addr_ln_1_txt
        and st.deliverycity = olc.city_nm
        and st.deliverystate = olc.state_nm
        and st.deliverycountryregionid = olc.country_nm
        and st.deliveryzipcode = olc.zip_cd
    left join __dbt__cte__v_ax_warehouse_curr wr 
        on upper(st.inventlocationid) = wr.warehouse_id
        and st.dataareaid = wr.opco_id
        and wr.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr ocust
        on st.dataareaid = ocust.opco_id
        and st.invoiceaccount = ocust.cust_id
        and ocust.src_sys_nm = 'AX'
    where st.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select 
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_id as 
    varchar
), '') as 
    varchar
)) as opco_sls_ordr_sk,
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_id as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_nm as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_dt as 
    varchar
), '') || '-' || coalesce(cast(ship_dt as 
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
), '') || '-' || coalesce(cast(opco_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_trans_status_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_grp_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_site_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dlvry_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dlvry_mode_sk as 
    varchar
), '') || '-' || coalesce(cast(dlvry_loc_sk as 
    varchar
), '') || '-' || coalesce(cast(warehouse_sk as 
    varchar
), '') || '-' || coalesce(cast(sls_grp_nm as 
    varchar
), '') || '-' || coalesce(cast(tax_grp_id as 
    varchar
), '') || '-' || coalesce(cast(quotation_id as 
    varchar
), '') || '-' || coalesce(cast(cust_po_nbr as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_pymnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_pymnt_mode_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cash_dscnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(cash_dscnt_pct as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_crt_dtm as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_modified_dtm as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_crt_by_nm as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_modified_by_nm as 
    varchar
), '') || '-' || coalesce(cast(on_hold_ind as 
    varchar
), '') || '-' || coalesce(cast(on_hold_by_nm as 
    varchar
), '') || '-' || coalesce(cast(on_hold_rsn_desc as 
    varchar
), '') || '-' || coalesce(cast(dscnt_pct as 
    varchar
), '') || '-' || coalesce(cast(end_cust_nm as 
    varchar
), '') || '-' || coalesce(cast(cust_ref_txt as 
    varchar
), '') || '-' || coalesce(cast(dlvry_cust_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_sls_ordr_doc_status_sk as 
    varchar
), '') || '-' || coalesce(cast(invoice_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(line_dscnt_cd as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_phase_cd as 
    varchar
), '') || '-' || coalesce(cast(trade_grp_cd as 
    varchar
), '') || '-' || coalesce(cast(trade_grp_nm as 
    varchar
), '') || '-' || coalesce(cast(trade_grp_module_txt as 
    varchar
), '') || '-' || coalesce(cast(trade_grp_type_txt as 
    varchar
), '') || '-' || coalesce(cast(rcpt_cnfrm_dt as 
    varchar
), '') || '-' || coalesce(cast(rcpt_rqst_dt as 
    varchar
), '') || '-' || coalesce(cast(sls_taker_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_sls_ordr_type_sk as 
    varchar
), '') || '-' || coalesce(cast(ship_cnfrm_dt as 
    varchar
), '') || '-' || coalesce(cast(ship_rqst_dt as 
    varchar
), '') || '-' || coalesce(cast(titan_job_nbr as 
    varchar
), '') || '-' || coalesce(cast(online_ordr_ind as 
    varchar
), '') || '-' || coalesce(cast(spcl_instr_txt as 
    varchar
), '') || '-' || coalesce(cast(tax_exempt_nbr as 
    varchar
), '') || '-' || coalesce(cast(commsn_grp_id as 
    varchar
), '') || '-' || coalesce(cast(commsn_grp_nm as 
    varchar
), '') || '-' || coalesce(cast(last_acceptance_dt as 
    varchar
), '') || '-' || coalesce(cast(fixed_due_dt as 
    varchar
), '') || '-' || coalesce(cast(backlog_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(backlog_sls_amt as 
    varchar
), '') || '-' || coalesce(cast(bid_dt as 
    varchar
), '') || '-' || coalesce(cast(print_invoice_ind as 
    varchar
), '') || '-' || coalesce(cast(sls_lead_cd as 
    varchar
), '') || '-' || coalesce(cast(cust_alt_nm as 
    varchar
), '') || '-' || coalesce(cast(dsptch_locn_nm as 
    varchar
), '') || '-' || coalesce(cast(equipment_type_txt as 
    varchar
), '') as 
    varchar
)) as opco_sls_ordr_ck, 
        concat_ws('|', src_sys_nm, opco_id, sls_ordr_id) as opco_sls_ordr_ak, 
        * 
    from ax_opco_sls_ordr 
)

select * from final   




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr ) 
    
