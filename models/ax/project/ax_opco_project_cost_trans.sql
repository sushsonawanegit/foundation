{% set _load = load_type('AX_OPCO_PROJECT_COST_TRANS') %}

with ax_opco_project_cost_trans as(		
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
    from {{source('AX_DEV', 'PROJCOSTTRANS')}} pt
    left join {{ref('ax_opco_curr')}} o    
        on pt.dataareaid = o.opco_id    
        and o.src_sys_nm = 'AX'  
	left join {{ref('ax_opco_project_curr')}} op 
        on pt.dataareaid = op.opco_id
		and pt.projid = op.project_id
        and op.src_sys_nm = 'AX'  		
    left join {{ref('ax_opco_cost_center_curr')}} ooc 
        on upper(pt.dimension) = ooc.src_cost_center_cd
        and ooc.src_sys_nm = 'AX'
    left join {{ref('ax_opco_dept_curr')}} od
        on upper(pt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
	left join {{ref('ax_opco_type_curr')}} ot 
        on upper(pt.dimension3_) = ot.src_type_cd	
        and ot.src_sys_nm = 'AX'  	
	left join {{ref('ax_opco_purpose_curr')}} opp
        on upper(pt.dimension4_) = opp.src_purpose_cd	
        and opp.src_sys_nm = 'AX'  		
	left join {{ref('ax_opco_lob_curr')}} ol 
        on upper(pt.dimension5_) = ol.src_lob_cd	
        and ol.src_sys_nm = 'AX'  	
	left join {{ref('ax_opco_uom_curr')}} ou 
        on upper(pt.unitid_opi) = ou.src_uom_cd	
        and ou.src_sys_nm = 'AX'  
	left join {{ref('ax_opco_currency_curr')}} tc 
        on upper(pt.currencyidcost) = tc.src_currency_cd
        and tc.src_sys_nm = 'AX'
	left join {{ref('ax_opco_project_catgry_curr')}} opc 
        on pt.dataareaid = opc.opco_id
		and pt.categoryid = opc.catgry_cd
        and opc.src_sys_nm = 'AX' 
	left join {{ ref('ax_opco_project_trans_status_curr')}} opts
		on pt.transstatus = opts.project_trans_status_cd
		and opts.src_sys_nm = 'AX'
	left join {{ ref('ax_opco_project_trans_origin_curr')}} opto
		on pt.transactionorigin = opto.project_trans_origin_cd
		and opto.src_sys_nm = 'AX'
	where pt.dataareaid not in {{ var('excluded_ax_companies')}}    
),
final as (															
    select distinct
			{{ dbt_utils.surrogate_key(['src_sys_nm','opco_id','project_cost_trans_id']) }} as opco_project_cost_trans_sk,
			{{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'project_cost_trans_id', 'opco_sk', 'opco_project_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_uom_sk', 'trans_currency_sk', 'opco_project_catgry_sk', 'opco_project_trans_status_sk', 'opco_project_trans_origin_sk', 'trans_dt', 'trans_desc', 'cost_gl_status_txt', 'tax_grp_id', 'voucher_nbr', 'invoice_nbr', 'document_nbr', 'ref_trans_id', 'calculated_ind', 'trans_qty', 'trans_currency_cost_price_amt', 'opco_currency_cost_price_amt', 'opco_currency_sls_price_amt', 'opco_currency_gl_sls_amt', 'opco_currency_gl_cost_amt', 'opco_currency_item_price_amt', 'opco_currency_total_std_cost_amt', 'opco_currency_total_cost_amt', 'opco_currency_total_revenue_amt', 'opco_currency_sls_tax_amt', 'opco_currency_freight_amt', 'opco_currency_cash_dscnt_amt', 'margin_contribution_pct']) }} as opco_project_cost_trans_ck,
			concat_ws('|', src_sys_nm, opco_id, project_cost_trans_id) as opco_project_cost_trans_ak,
            * 		
    from ax_opco_project_cost_trans 															
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_PROJECT_COST_TRANS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PROJECT_COST_TRANS') and _load != 3%}
    union
    select
    OPCO_PROJECT_COST_TRANS_sk, OPCO_PROJECT_COST_TRANS_ck, OPCO_PROJECT_COST_TRANS_ak, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, opco_id, project_cost_trans_id, opco_sk, opco_project_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_uom_sk, trans_currency_sk, opco_project_catgry_sk, opco_project_trans_status_sk, opco_project_trans_origin_sk, trans_dt, trans_desc, cost_gl_status_txt, tax_grp_id, voucher_nbr, invoice_nbr, document_nbr, ref_trans_id, calculated_ind, trans_qty, trans_currency_cost_price_amt, opco_currency_cost_price_amt, opco_currency_sls_price_amt, opco_currency_gl_sls_amt, opco_currency_gl_cost_amt, opco_currency_item_price_amt, opco_currency_total_std_cost_amt, opco_currency_total_cost_amt, opco_currency_total_revenue_amt, opco_currency_sls_tax_amt, opco_currency_freight_amt, opco_currency_cash_dscnt_amt, margin_contribution_pct

    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_COST_TRANS
    {% if _load == 1 %}
        where OPCO_PROJECT_COST_TRANS_ck not in (select distinct OPCO_PROJECT_COST_TRANS_ck from final)
    {% else %}
        where OPCO_PROJECT_COST_TRANS_ck not in (select distinct OPCO_PROJECT_COST_TRANS_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_COST_TRANS where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PROJECT_COST_TRANS_ck not in (select distinct OPCO_PROJECT_COST_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_COST_TRANS where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PROJECT_COST_TRANS_ck in (select distinct OPCO_PROJECT_COST_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_COST_TRANS where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}