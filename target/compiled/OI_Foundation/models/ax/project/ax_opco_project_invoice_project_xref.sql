

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
),ax_opco_project_invoice_project_xref as(  
    select 
    op1.opco_project_sk,
    op2.opco_project_sk as invoice_project_sk,
    current_timestamp as crt_dtm,
    pt._fivetran_synced as stg_load_dtm,
	case 
	    when pt._fivetran_deleted = true then pt._fivetran_synced
	    else null
	end as delete_dtm,
	'AX' as src_sys_nm,
	pt.dataareaid as opco_id,
	pt.projinvoiceprojid as invoice_project_id
    from FIVETRAN.AX_PRODREPLICATION1_DBO.PROJTABLE pt
    left join __dbt__cte__v_ax_opco_project_curr op1    
        on pt.dataareaid = op1.opco_id   
		and pt.projid = op1.project_id		
        and op1.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_project_curr op2    
        on pt.dataareaid = op2.opco_id   
		and pt.projinvoiceprojid = op2.project_id		
        and op2.src_sys_nm = 'AX'
	where pt.dataareaid not in ('044', '045', '047', '999') 
),
final as (															
    select 
        opco_project_sk, invoice_project_sk,
        md5(cast(coalesce(cast(opco_project_sk as 
    varchar
), '') || '-' || coalesce(cast(invoice_project_sk as 
    varchar
), '') || '-' || coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(invoice_project_id as 
    varchar
), '') as 
    varchar
)) as opco_project_invoice_project_xref_ck,
        crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, opco_id, invoice_project_id 		
    from ax_opco_project_invoice_project_xref  
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_invoice_project_xref ) 
    
