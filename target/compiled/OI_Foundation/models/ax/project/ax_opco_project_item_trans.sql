

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
),  __dbt__cte__v_ax_opco_project_trans_status_curr as (
with v_ax_opco_project_trans_status as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_trans_status
)

select * from v_ax_opco_project_trans_status
),  __dbt__cte__v_ax_opco_project_trans_origin_curr as (
with v_ax_opco_project_trans_origin as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_trans_origin

)

select * from v_ax_opco_project_trans_origin
),ax_opco_project_item_trans as (
    select
    current_timestamp as crt_dtm,
    pit._fivetran_synced as stg_load_dtm,
    case
        when pit._fivetran_deleted = true then pit._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    pit.dataareaid as opco_id,
    pit.projtransid as project_item_trans_id,
    pit.inventtransid as invtry_trans_id,
    opr.opco_project_sk,
    opco.opco_sk,
    oi.opco_item_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    opc.opco_project_catgry_sk,
    ou.opco_uom_sk as trans_uom_sk,
    oa.opco_assctn_sk,
    opts.opco_project_trans_status_sk,
    opto.opco_project_trans_origin_sk,
    pit.transdate as trans_dt,
    pit.txt as trans_desc,
    pit.linepropertyid as line_property_id,
    pit.taxgroupid as tax_grp_id,
    taxitemgroupid as tax_item_grp_id,
    voucherpackingslip as packing_slip_voucher_nbr,
    salesinvoiceid as cust_invoice_nbr,
    projadjustrefid as project_adjsmt_ref_id,
    projtransidref as ref_project_item_trans_id,
    calculated_opi as calc_ind,
    convertedfromcosttrans_opi as project_cost_trans_conversion_ind,
    qty as trans_qty,
    salesprice as per_unit_sls_price_amt,
    itemprice_opi as item_price_amt,
    totalrevenue_opi as total_revenue_amt,
    totalcost_opi as total_cost_amt,
    totalstdcost_opi as total_std_cost_amt,
    mcpercent_opi as margin_contribution_pct
    from FIVETRAN.AX_PRODREPLICATION1_DBO.PROJITEMTRANS pit
    left join __dbt__cte__v_ax_opco_project_curr opr
        on pit.dataareaid = opr.opco_id
        and pit.projid = opr.project_id
        and opr.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_curr opco 
        on pit.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_item_curr oi 
        on pit.dataareaid = oi.opco_id
        and pit.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(pit.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(pit.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(pit.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr op 
        on upper(pit.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol
        on upper(pit.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_project_catgry_curr opc
        on pit.dataareaid = opc.opco_id
        and pit.categoryid = opc.catgry_cd
        and opc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_uom_curr ou 
        on upper(pit.salesunit) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_assctn_curr oa 
        on pit.dataareaid = oa.opco_id
        and pit.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_project_trans_status_curr opts
		on pit.transstatus = opts.project_trans_status_cd
		and opts.src_sys_nm = 'AX'
	left join __dbt__cte__v_ax_opco_project_trans_origin_curr opto
		on pit.transactionorigin = opto.project_trans_origin_cd
		and opto.src_sys_nm = 'AX'
    where pit.dataareaid not in ('044', '045', '047', '999')
    and pit.projtransid is not null
    and pit.projtransid <> ''
),
final as (															
    select 
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(project_item_trans_id as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_id as 
    varchar
), '') as 
    varchar
)) as opco_project_item_trans_sk,
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(project_item_trans_id as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_id as 
    varchar
), '') || '-' || coalesce(cast(opco_project_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_sk as 
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
), '') || '-' || coalesce(cast(trans_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_assctn_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_project_trans_status_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_project_trans_origin_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_dt as 
    varchar
), '') || '-' || coalesce(cast(trans_desc as 
    varchar
), '') || '-' || coalesce(cast(line_property_id as 
    varchar
), '') || '-' || coalesce(cast(tax_grp_id as 
    varchar
), '') || '-' || coalesce(cast(tax_item_grp_id as 
    varchar
), '') || '-' || coalesce(cast(packing_slip_voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(cust_invoice_nbr as 
    varchar
), '') || '-' || coalesce(cast(project_adjsmt_ref_id as 
    varchar
), '') || '-' || coalesce(cast(ref_project_item_trans_id as 
    varchar
), '') || '-' || coalesce(cast(calc_ind as 
    varchar
), '') || '-' || coalesce(cast(project_cost_trans_conversion_ind as 
    varchar
), '') || '-' || coalesce(cast(trans_qty as 
    varchar
), '') || '-' || coalesce(cast(per_unit_sls_price_amt as 
    varchar
), '') || '-' || coalesce(cast(item_price_amt as 
    varchar
), '') || '-' || coalesce(cast(total_revenue_amt as 
    varchar
), '') || '-' || coalesce(cast(total_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(total_std_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(margin_contribution_pct as 
    varchar
), '') as 
    varchar
)) as opco_project_item_trans_ck,
        concat_ws('|', src_sys_nm, opco_id, project_item_trans_id, invtry_trans_id) as opco_project_item_trans_ak,
        * 		
    from ax_opco_project_item_trans 															
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_item_trans ) 
    
