

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
),  __dbt__cte__v_ax_opco_gl_trans_type_curr as (
with v_ax_opco_gl_trans_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_gl_trans_type
)

select * from v_ax_opco_gl_trans_type
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
),  __dbt__cte__v_ax_opco_project_curr as (
with v_ax_opco_project as(
    select *,
    rank() over(partition by opco_project_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project
),
final as(
    select 
    *
    from v_ax_opco_project
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_cust_trans as (
    select
    current_timestamp as crt_dtm,
    ct._fivetran_synced as stg_load_dtm,
    case 
        when ct._fivetran_deleted = true then ct._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    ct.dataareaid as opco_id,
    ct.recid as src_key_txt,
    opco.opco_sk,
    oc.opco_cust_sk,
    odc.opco_cust_sk as order_cust_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    oso.opco_sls_ordr_sk,
    ogt.opco_gl_trans_type_sk,
    ocu.opco_currency_sk as trans_currency_sk,
    opm.opco_pymnt_mode_sk,
    opt.opco_pymnt_terms_sk,
    ocd.opco_cash_dscnt_terms_sk,
    odm.opco_dlvry_mode_sk,
    opr.opco_project_sk,
    ct.transdate as trans_dt,
    ct.approved as trans_apprvd_ind,
    ct.voucher as voucher_nbr,
    ct.invoice as invoice_nbr,
    ct.offsetrecid as offset_src_key_txt,
    ct.duedate as due_dt,
    ct.correct as crrctn_trans_ind,
    ct.settlement as auto_settle_ind,
    ct.cancelledpayment as pymnt_cancelled_ind,
    ct.interest as interest_calc_allowed_ind,
    ct.collectionletter as collection_letter_allowed_ind,
    ct.cashpayment as cod_cash_pymnt_ind,
    ct.paymreference as pymnt_ref_txt,
    case 
        when ct.paymmethod = 0 then 'Net'
        when ct.paymmethod = 1 then 'Current Month'
        when ct.paymmethod = 2 then 'Current Quarter'
        when ct.paymmethod = 3 then 'Current Year'
        when ct.paymmethod = 4 then 'Current Week'
        when ct.paymmethod = 5 then 'C.O.D.'
    end as pymnt_method_txt,
    ct.retention_opi as retention_cd,
    ct.retentioncheck_opi as check_retention_ind,
    ct.documentnum as document_nbr,
    ct.documentdate as document_dt,
    ct.discdate_opi as trans_dscnt_dt,
    ct.lastinterestdate_opi as ltst_interest_dt,
    case
        when ct.collectionlettercode = 0 then 'None'
        when ct.collectionlettercode = 1 then 'Collection Letter 1'
        when ct.collectionlettercode = 2 then 'Collection Letter 2'
        when ct.collectionlettercode = 3 then 'Collection Letter 3'
        when ct.collectionlettercode = 4 then 'Collection Letter 4'
        when ct.collectionlettercode = 5 then 'Collection'
        when ct.collectionlettercode = 6 then 'All'
        when ct.collectionlettercode = 7 then 'Collection Per Customer'
    end as collection_letter_txt,
    ct.purchaseorder_opi as po_form_nbr,
    ct.lastsettlecompany as ltst_settled_opco_sk,
    ct.lastsettleaccountnum as ltst_settled_cust_sk,
    ct.lastsettledate as ltst_settled_dt,
    ct.lastsettlevoucher as ltst_settled_voucher_nbr,
    ct.closed as trans_closed_dt,
    ct.amountcur as trans_currency_trans_amt,
    ct.amountmst as opco_currency_trans_amt,
    ct.settleamountcur as trans_currency_total_settled_amt,
    ct.settleamountmst as opco_currency_total_settled_amt,
    ct.discamt_opi as trans_dscnt_amt,
    ct.sumtax_opi as sum_tax_amt,
    ct.exchrate as trans_exch_rt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTTRANS ct 
    left join __dbt__cte__v_ax_opco_curr opco 
        on ct.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr oc 
        on ct.accountnum = oc.cust_id
        and ct.dataareaid = oc.opco_id
        and oc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr odc 
        on ct.orderaccount = odc.cust_id
        and ct.dataareaid = odc.opco_id
        and odc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(ct.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(ct.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(ct.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr op 
        on upper(ct.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol 
        on upper(ct.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_sls_ordr_curr oso 
        on ct.dataareaid = oso.opco_id
        and ct.salesid_opi = oso.sls_ordr_id
        and oso.src_sys_nm = 'AX'
        and ct.salesid_opi is not null
        and ct.salesid_opi <> '0'
    left join __dbt__cte__v_ax_opco_gl_trans_type_curr ogt 
        on ct.transtype::string =  ogt.src_gl_trans_type_cd
        and ogt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr ocu 
        on upper(ct.currencycode) = ocu.src_currency_cd
        and ocu.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_pymnt_mode_curr opm 
        on upper(ct.paymmode) = opm.src_pymnt_mode_cd
        and opm.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cash_dscnt_terms_curr ocd
        on upper(ct.cashdisccode) = ocd.src_cash_dscnt_terms_cd
        and ocd.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_pymnt_terms_curr opt 
        on upper(ct.payment_opi) = opt.src_pymnt_terms_cd
        and opt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dlvry_mode_curr odm 
        on upper(ct.deliverymode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_project_curr opr 
        on ct.projid_opi = opr.project_id
        and ct.dataareaid = opr.opco_id
        and opr.src_sys_nm = 'AX'
    where ct.dataareaid not in ('044', '045', '047', '999')
),
final as(
    select distinct
           md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') as 
    varchar
)) as opco_cust_trans_sk,
           md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_sk as 
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
), '') || '-' || coalesce(cast(opco_sls_ordr_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_gl_trans_type_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_pymnt_mode_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_pymnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cash_dscnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dlvry_mode_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_project_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_dt as 
    varchar
), '') || '-' || coalesce(cast(trans_apprvd_ind as 
    varchar
), '') || '-' || coalesce(cast(voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(invoice_nbr as 
    varchar
), '') || '-' || coalesce(cast(offset_src_key_txt as 
    varchar
), '') || '-' || coalesce(cast(due_dt as 
    varchar
), '') || '-' || coalesce(cast(crrctn_trans_ind as 
    varchar
), '') || '-' || coalesce(cast(auto_settle_ind as 
    varchar
), '') || '-' || coalesce(cast(pymnt_cancelled_ind as 
    varchar
), '') || '-' || coalesce(cast(interest_calc_allowed_ind as 
    varchar
), '') || '-' || coalesce(cast(collection_letter_allowed_ind as 
    varchar
), '') || '-' || coalesce(cast(cod_cash_pymnt_ind as 
    varchar
), '') || '-' || coalesce(cast(pymnt_ref_txt as 
    varchar
), '') || '-' || coalesce(cast(pymnt_method_txt as 
    varchar
), '') || '-' || coalesce(cast(retention_cd as 
    varchar
), '') || '-' || coalesce(cast(check_retention_ind as 
    varchar
), '') || '-' || coalesce(cast(document_nbr as 
    varchar
), '') || '-' || coalesce(cast(document_dt as 
    varchar
), '') || '-' || coalesce(cast(trans_dscnt_dt as 
    varchar
), '') || '-' || coalesce(cast(ltst_interest_dt as 
    varchar
), '') || '-' || coalesce(cast(collection_letter_txt as 
    varchar
), '') || '-' || coalesce(cast(po_form_nbr as 
    varchar
), '') || '-' || coalesce(cast(ltst_settled_opco_sk as 
    varchar
), '') || '-' || coalesce(cast(ltst_settled_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(ltst_settled_dt as 
    varchar
), '') || '-' || coalesce(cast(ltst_settled_voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(trans_closed_dt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_trans_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_trans_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_total_settled_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_total_settled_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_dscnt_amt as 
    varchar
), '') || '-' || coalesce(cast(sum_tax_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_exch_rt as 
    varchar
), '') as 
    varchar
)) as opco_cust_trans_ck,
           concat_ws('|', src_sys_nm, opco_id, src_key_txt) as opco_cust_trans_ak, *
    from ax_opco_cust_trans 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_trans ) 
    
