

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
),ax_opco_project_cost_trans as(		
    select 
    current_timestamp as crt_dtm,
    pt._fivetran_synced as stg_load_dtm,
	case 
	    when pt._fivetran_deleted = true then pt._fivetran_synced
        else null
	end as delete_dtm,
	'AX' as src_sys_nm,
	pt.dataareaid as opco_id,
	pt.transid as project_cost_trans_id,
	o.opco_sk,
	op.opco_project_sk,
	ooc.opco_cost_center_sk,
	od.opco_dept_sk,
	ot.opco_type_sk,
	opp.opco_purpose_sk,
	ol.opco_lob_sk,
	ou.opco_uom_sk,
	tc.opco_currency_sk as trans_currency_sk,
	opc.opco_project_catgry_sk,
	opts.opco_project_trans_status_sk,
	opto.opco_project_trans_origin_sk,
	pt.transdate as trans_dt,
	pt.txt as trans_desc,
	case 
        when pt.ledgerstatuscost = 0 then 'No Ledger'
        when pt.ledgerstatuscost = 1 then 'Balance'
        when pt.ledgerstatuscost = 2 then 'Profit and Loss'
        when pt.ledgerstatuscost = 3 then 'Never Ledger'
    end as cost_gl_status_txt,
    pt.taxgroupid as tax_grp_id,			
	pt.voucherjournal as voucher_nbr,			
	pt.invoiceid_opi as invoice_nbr,			
	pt.documentnum_opi as document_nbr,		
	pt.transidref as ref_trans_id,		
	pt.calculated_opi as calculated_ind,	
	pt.qty as trans_qty,						 
	pt.costpricecurrency  as trans_currency_cost_price_amt,	 
	pt.costprice as opco_currency_cost_price_amt,	 
	pt.salesprice as opco_currency_sls_price_amt,		 
	pt.ledgersalesamount  as opco_currency_gl_sls_amt,		 
	pt.costamountledger as opco_currency_gl_cost_amt,		 
	pt.itemprice_opi as opco_currency_item_price_amt,	 
	pt.totalstdcost_opi as opco_currency_total_std_cost_amt, 
	pt.totalcost_opi  as opco_currency_total_cost_amt,	 
	pt.totalrevenue_opi as opco_currency_total_revenue_amt,	 
	pt.salestaxamount_opi as opco_currency_sls_tax_amt,		 
	pt.freightamount_opi as opco_currency_freight_amt,		 
	pt.cashdiscamount_opi as opco_currency_cash_dscnt_amt,	 
	pt.mcpercent_opi as margin_contribution_pct
    from FIVETRAN.AX_PRODREPLICATION1_DBO.PROJCOSTTRANS pt
    left join __dbt__cte__v_ax_opco_curr o    
        on pt.dataareaid = o.opco_id    
        and o.src_sys_nm = 'AX'  
	left join __dbt__cte__v_ax_opco_project_curr op 
        on pt.dataareaid = op.opco_id
		and pt.projid = op.project_id
        and op.src_sys_nm = 'AX'  		
    left join __dbt__cte__v_ax_opco_cost_center_curr ooc 
        on upper(pt.dimension) = ooc.src_cost_center_cd
        and ooc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od
        on upper(pt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
	left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(pt.dimension3_) = ot.src_type_cd	
        and ot.src_sys_nm = 'AX'  	
	left join __dbt__cte__v_ax_opco_purpose_curr opp
        on upper(pt.dimension4_) = opp.src_purpose_cd	
        and opp.src_sys_nm = 'AX'  		
	left join __dbt__cte__v_ax_opco_lob_curr ol 
        on upper(pt.dimension5_) = ol.src_lob_cd	
        and ol.src_sys_nm = 'AX'  	
	left join __dbt__cte__v_ax_opco_uom_curr ou 
        on upper(pt.unitid_opi) = ou.src_uom_cd	
        and ou.src_sys_nm = 'AX'  
	left join __dbt__cte__v_ax_opco_currency_curr tc 
        on upper(pt.currencyidcost) = tc.src_currency_cd
        and tc.src_sys_nm = 'AX'
	left join __dbt__cte__v_ax_opco_project_catgry_curr opc 
        on pt.dataareaid = opc.opco_id
		and pt.categoryid = opc.catgry_cd
        and opc.src_sys_nm = 'AX' 
	left join __dbt__cte__v_ax_opco_project_trans_status_curr opts
		on pt.transstatus = opts.project_trans_status_cd
		and opts.src_sys_nm = 'AX'
	left join __dbt__cte__v_ax_opco_project_trans_origin_curr opto
		on pt.transactionorigin = opto.project_trans_origin_cd
		and opto.src_sys_nm = 'AX'
	where pt.dataareaid not in ('044', '045', '047', '999')    
),
final as (															
    select distinct
			md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(project_cost_trans_id as 
    varchar
), '') as 
    varchar
)) as opco_project_cost_trans_sk,
			md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(project_cost_trans_id as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_project_sk as 
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
), '') || '-' || coalesce(cast(opco_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_project_catgry_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_project_trans_status_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_project_trans_origin_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_dt as 
    varchar
), '') || '-' || coalesce(cast(trans_desc as 
    varchar
), '') || '-' || coalesce(cast(cost_gl_status_txt as 
    varchar
), '') || '-' || coalesce(cast(tax_grp_id as 
    varchar
), '') || '-' || coalesce(cast(voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(invoice_nbr as 
    varchar
), '') || '-' || coalesce(cast(document_nbr as 
    varchar
), '') || '-' || coalesce(cast(ref_trans_id as 
    varchar
), '') || '-' || coalesce(cast(calculated_ind as 
    varchar
), '') || '-' || coalesce(cast(trans_qty as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_cost_price_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_cost_price_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_sls_price_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_gl_sls_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_gl_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_item_price_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_total_std_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_total_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_total_revenue_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_sls_tax_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_freight_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_cash_dscnt_amt as 
    varchar
), '') || '-' || coalesce(cast(margin_contribution_pct as 
    varchar
), '') as 
    varchar
)) as opco_project_cost_trans_ck,
			concat_ws('|', src_sys_nm, opco_id, project_cost_trans_id) as opco_project_cost_trans_ak,
            * 		
    from ax_opco_project_cost_trans 															
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_cost_trans ) 
    
