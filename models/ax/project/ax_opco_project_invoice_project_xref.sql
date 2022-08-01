{% set _load = load_type('AX_OPCO_PROJECT_INVOICE_PROJECT_XREF') %}

with ax_opco_project_invoice_project_xref as(  
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
    from {{source('AX_DEV', 'PROJTABLE')}} pt
    left join {{ref('ax_opco_project_curr')}} op1    
        on pt.dataareaid = op1.opco_id   
		and pt.projid = op1.project_id		
        and op1.src_sys_nm = 'AX'
    left join {{ref('ax_opco_project_curr')}} op2    
        on pt.dataareaid = op2.opco_id   
		and pt.projinvoiceprojid = op2.project_id		
        and op2.src_sys_nm = 'AX'
	where pt.dataareaid not in {{ var('excluded_ax_companies')}} 
),
final as (															
    select 
        opco_project_sk, invoice_project_sk,
        {{ dbt_utils.surrogate_key([ 'opco_project_sk',  'invoice_project_sk',  'src_sys_nm',  'opco_id',  'invoice_project_id'])}} as opco_project_invoice_project_xref_ck,
        crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, opco_id, invoice_project_id 		
    from ax_opco_project_invoice_project_xref  
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_PROJECT_INVOICE_PROJECT_XREF') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PROJECT_INVOICE_PROJECT_XREF') and _load != 3%}
    union
    select
    opco_project_sk, invoice_project_sk, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, opco_id, invoice_project_id
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_INVOICE_PROJECT_XREF
    {% if _load == 1 %}
        where OPCO_PROJECT_INVOICE_PROJECT_XREF_ck not in (select distinct OPCO_PROJECT_INVOICE_PROJECT_XREF_ck from final)
    {% else %}
        where OPCO_PROJECT_INVOICE_PROJECT_XREF_ck not in (select distinct OPCO_PROJECT_INVOICE_PROJECT_XREF_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_INVOICE_PROJECT_XREF where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PROJECT_INVOICE_PROJECT_XREF_ck not in (select distinct OPCO_PROJECT_INVOICE_PROJECT_XREF_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_INVOICE_PROJECT_XREF where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PROJECT_INVOICE_PROJECT_XREF_ck in (select distinct OPCO_PROJECT_INVOICE_PROJECT_XREF_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_INVOICE_PROJECT_XREF where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}