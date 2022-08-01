

with  __dbt__cte__v_ax_opco_project_curr as (
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
),  __dbt__cte__v_ax_opco_curr as (
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
),  __dbt__cte__v_ax_opco_project_catgry_curr as (
with v_ax_opco_project_catgry as(
    select *,
    rank() over(partition by opco_project_catgry_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_catgry
),
final as(
    select 
    *
    from v_ax_opco_project_catgry
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
),ax_opco_project_cost_frcst as (
    select
    current_timestamp as crt_dtm,
    pfc._fivetran_synced as stg_load_dtm,
    case
        when pfc._fivetran_deleted = true then pfc._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    pfc.dataareaid as opco_id,
    pfc.transid as project_cost_frcst_id,
    opr.opco_project_sk,
    opco.opco_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    opc.opco_project_catgry_sk,
    ocr.opco_currency_sk as trans_currency_sk,
    ou.opco_uom_sk,
    pfc.linepropertyid as line_property_id,
    pfc.invoicedate as invoice_expctd_dt,
    pfc.startdate as budget_amt_applctn_dt,
    pfc.salespaymdate as revenue_pymnt_expctd_dt,
    pfc.costpaymdate as cost_pymnt_expctd_dt,
    pfc.eliminationdate as elimination_expctd_dt,
    pfc.modelid as frcst_model_id,
    pfc.taxgroupid as tax_grp_id,
    pfc.report as include_in_rpt_ind,
    pfc.islock_opi as lock_ind,
    pfc.active as include_amt_ind,
    pfc.qty as trans_qty,
    pfc.costprice as opco_currency_cost_price_amt,
    pfc.salesprice as trans_currency_sls_price_amt,
    pfc.totalrevenue_opi as total_revenue_amt,
    pfc.totalcost_opi as total_cost_amt,
    pfc.mcpercent_opi as marging_contribution_pct
    from FIVETRAN.AX_PRODREPLICATION1_DBO.PROJFORECASTCOST pfc
    left join __dbt__cte__v_ax_opco_project_curr opr
        on pfc.dataareaid = opr.opco_id
        and pfc.projid = opr.project_id
        and opr.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_curr opco 
        on pfc.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(pfc.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(pfc.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(pfc.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr op 
        on upper(pfc.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol
        on upper(pfc.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_project_catgry_curr opc
        on pfc.dataareaid = opc.opco_id
        and pfc.categoryid = opc.catgry_cd
        and opc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr ocr
        on upper(pfc.currencyid) = ocr.src_currency_cd
        and ocr.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_uom_curr ou 
        on upper(pfc.unitid_opi) = ou.src_uom_cd
        and ou.src_sys_nm = 'Ax'
    where pfc.dataareaid not in ('044', '045', '047', '999')
),
final as (															
    select distinct
			md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(project_cost_frcst_id as 
    varchar
), '') as 
    varchar
)) as opco_project_cost_frcst_sk,
			md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(project_cost_frcst_id as 
    varchar
), '') || '-' || coalesce(cast(opco_project_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
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
), '') || '-' || coalesce(cast(opco_project_catgry_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(line_property_id as 
    varchar
), '') || '-' || coalesce(cast(invoice_expctd_dt as 
    varchar
), '') || '-' || coalesce(cast(budget_amt_applctn_dt as 
    varchar
), '') || '-' || coalesce(cast(revenue_pymnt_expctd_dt as 
    varchar
), '') || '-' || coalesce(cast(cost_pymnt_expctd_dt as 
    varchar
), '') || '-' || coalesce(cast(elimination_expctd_dt as 
    varchar
), '') || '-' || coalesce(cast(frcst_model_id as 
    varchar
), '') || '-' || coalesce(cast(tax_grp_id as 
    varchar
), '') || '-' || coalesce(cast(include_in_rpt_ind as 
    varchar
), '') || '-' || coalesce(cast(lock_ind as 
    varchar
), '') || '-' || coalesce(cast(include_amt_ind as 
    varchar
), '') || '-' || coalesce(cast(trans_qty as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_cost_price_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sls_price_amt as 
    varchar
), '') || '-' || coalesce(cast(total_revenue_amt as 
    varchar
), '') || '-' || coalesce(cast(total_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(marging_contribution_pct as 
    varchar
), '') as 
    varchar
)) as opco_project_cost_frcst_ck,
			concat_ws('|', src_sys_nm, opco_id, project_cost_frcst_id) as opco_project_cost_frcst_ak,
            * 		
    from ax_opco_project_cost_frcst 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_cost_frcst ) 
    
