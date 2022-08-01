

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
),  __dbt__cte__v_ax_opco_cust_code_curr as (
with v_ax_opco_cust_code as(
    select *,
    rank() over(partition by opco_cust_code_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_code
),
final as(
    select 
    *
    from v_ax_opco_cust_code
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_cust_type_curr as (
with v_ax_opco_cust_type as(
    select *,
    rank() over(partition by opco_cust_type_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_type
),
final as(
    select 
    *
    from v_ax_opco_cust_type
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_commsn_grp_curr as (
with v_ax_opco_commsn_grp as(
    select *,
    rank() over(partition by opco_commsn_grp_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_commsn_grp
),
final as(
    select 
    *
    from v_ax_opco_commsn_grp
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
),ax_opco_opco_cust_xref as(
    select 
    opco.opco_sk,
    oc.opco_cust_sk,
    current_timestamp as crt_dtm,
    ct._fivetran_synced as stg_load_dtm,
    case
        when ct._fivetran_deleted = true then ct._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    1 as actv_ind,
    opcur.opco_currency_sk,
    oppyt.opco_pymnt_terms_sk,
    oppym.opco_pymnt_mode_sk,
    opcc.opco_cust_code_sk,
    opct.opco_cust_type_sk,
    opcg.opco_commsn_grp_sk,
    opcug.opco_cust_grp_sk,
    opcdt.opco_cash_dscnt_terms_sk,
    upper(ct.linedisc) as line_disc_txt,
    ct.creditmax as max_cr_amt,
    ct.mandatorycreditlimit as mandatory_cr_lmt_ind,
    ct.blocked as block_lvl_cd,
    ct.custclassificationid as cust_clssfctn_cd,
    upper(ct.paymsched) as pymnt_schd_desc,
    ct.salesgroup as sls_grp_nm,
    ct.regsalesmanager_opi as rgnl_sls_mgr_nm,
    ct.invoiceaccount as invoice_acct_nbr,
    ct.segment_opi as segment_cd,
    ct.segmentation_opi as segmentation_txt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTTABLE ct
    left join __dbt__cte__v_ax_opco_curr opco
        on ct.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr oc 
        on ct.accountnum = oc.cust_id
        and ct.dataareaid = oc.opco_id
        and oc.src_sys_nm = 'AX' 
    left join __dbt__cte__v_ax_opco_currency_curr opcur
        on upper(ct.currency) = opcur.src_currency_cd
        and opcur.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_pymnt_terms_curr oppyt
        on upper(ct.paymtermid) = oppyt.src_pymnt_terms_cd
        and oppyt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_pymnt_mode_curr oppym
        on  upper(ct.paymmode) = oppym.src_pymnt_mode_cd 
        and oppym.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_code_curr opcc 
        on upper(ct.precastcode_opi) = opcc.src_cust_code_cd 
        and opcc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_type_curr opct
        on upper(ct.precastcategory_opi) = opct.src_cust_type_cd
        and opct.src_sys_nm = 'AX' 
    left join __dbt__cte__v_ax_opco_commsn_grp_curr opcg 
        on upper(ct.commissiongroup) = opcg.src_commsn_grp_cd
        and opcg.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_grp_curr opcug
        on upper(ct.custgroup) = opcug.src_cust_grp_cd 
        and opcug.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cash_dscnt_terms_curr opcdt 
        on upper(cashdisc) = opcdt.src_cash_dscnt_terms_cd
        and opcdt.src_sys_nm = 'AX' 
    where dataareaid not in ('044', '045', '047', '999')
),
final as(
    select
        md5(cast(coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_pymnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_pymnt_mode_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_code_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_commsn_grp_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_grp_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cash_dscnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(line_disc_txt as 
    varchar
), '') || '-' || coalesce(cast(max_cr_amt as 
    varchar
), '') || '-' || coalesce(cast(mandatory_cr_lmt_ind as 
    varchar
), '') || '-' || coalesce(cast(block_lvl_cd as 
    varchar
), '') || '-' || coalesce(cast(cust_clssfctn_cd as 
    varchar
), '') || '-' || coalesce(cast(pymnt_schd_desc as 
    varchar
), '') || '-' || coalesce(cast(sls_grp_nm as 
    varchar
), '') || '-' || coalesce(cast(rgnl_sls_mgr_nm as 
    varchar
), '') || '-' || coalesce(cast(invoice_acct_nbr as 
    varchar
), '') || '-' || coalesce(cast(segment_cd as 
    varchar
), '') || '-' || coalesce(cast(segmentation_txt as 
    varchar
), '') as 
    varchar
)) as opco_opco_cust_xref_ck,
        *
    from ax_opco_opco_cust_xref
)

select * from final



    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_opco_cust_xref ) 
    
