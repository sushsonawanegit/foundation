

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
),ax_opco_project_empl_trans as (
    select
    current_timestamp as crt_dtm,
    pet._fivetran_synced as stg_load_dtm,
    case
        when pet._fivetran_deleted = true then pet._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    pet.dataareaid as opco_id,
    pet.transid as project_empl_trans_id,
    opr.opco_project_sk,
    opco.opco_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    opc.opco_project_catgry_sk,
    opts.opco_project_trans_status_sk,
    opto.opco_project_trans_origin_sk,
    ocr.opco_currency_sk as trans_currency_sk,
    pet.emplid as empl_id,
    pet.linepropertyid as line_property_id,
    pet.transidref as ref_project_empl_trans_id,
    pet.taxgroupid as tax_grp_id,
    pet.transdate as trans_dt,
    pet.voucherjournal as gl_voucher_nbr,
    case
        when pet.ledgerstatuscost = 0 then 'No Ledger'
        when pet.ledgerstatuscost = 1 then 'Balance'
        when pet.ledgerstatuscost = 2 then 'Profit and Loss'
        when pet.ledgerstatuscost = 3 then 'Never Ledger'
        when pet.ledgerstatuscost = 100 then 'Undefined'
    end as gl_section_txt,
    pet.txt as project_empl_trans_desc,
    pet.calculated_opi as calc_ind,
    pet.qty as trans_hour_qty,
    pet.costprice as opco_currency_hrly_cost_price_amt,
    pet.salesprice as trans_currency_hrly_sls_price_amt,
    pet.itemprice_opi as item_price_amt,
    pet.totalrevenue_opi as total_revenue_amt,
    pet.totalcost_opi as total_cost_amt,
    pet.totalstdcost_opi as total_std_cost_amt,
    pet.mcpercent_opi as margin_contribution_pct
    from FIVETRAN.AX_PRODREPLICATION1_DBO.PROJEMPLTRANS pet
    left join __dbt__cte__v_ax_opco_project_curr opr
        on pet.dataareaid = opr.opco_id
        and pet.projid = opr.project_id
        and opr.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_curr opco 
        on pet.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(pet.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(pet.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(pet.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr op 
        on upper(pet.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol
        on upper(pet.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_project_catgry_curr opc
        on pet.dataareaid = opc.opco_id
        and pet.categoryid = opc.catgry_cd
        and opc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_project_trans_status_curr opts
		on pet.transstatus = opts.project_trans_status_cd
		and opts.src_sys_nm = 'AX'
	left join __dbt__cte__v_ax_opco_project_trans_origin_curr opto
		on pet.transactionorigin = opto.project_trans_origin_cd
		and opto.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr ocr
        on upper(pet.currencyid) = ocr.src_currency_cd
        and ocr.src_sys_nm = 'AX'
    where pet.dataareaid not in ('044', '045', '047', '999')
),
final as (															
    select 
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(project_empl_trans_id as 
    varchar
), '') as 
    varchar
)) as opco_project_empl_trans_sk,
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(project_empl_trans_id as 
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
), '') || '-' || coalesce(cast(opco_project_trans_status_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_project_trans_origin_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(empl_id as 
    varchar
), '') || '-' || coalesce(cast(line_property_id as 
    varchar
), '') || '-' || coalesce(cast(ref_project_empl_trans_id as 
    varchar
), '') || '-' || coalesce(cast(tax_grp_id as 
    varchar
), '') || '-' || coalesce(cast(trans_dt as 
    varchar
), '') || '-' || coalesce(cast(gl_voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(gl_section_txt as 
    varchar
), '') || '-' || coalesce(cast(project_empl_trans_desc as 
    varchar
), '') || '-' || coalesce(cast(calc_ind as 
    varchar
), '') || '-' || coalesce(cast(trans_hour_qty as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_hrly_cost_price_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_hrly_sls_price_amt as 
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
)) as opco_project_empl_trans_ck,
        concat_ws('|', src_sys_nm, opco_id, project_empl_trans_id) as opco_project_empl_trans_ak,
        * 		
    from ax_opco_project_empl_trans opet															
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_empl_trans ) 
    
