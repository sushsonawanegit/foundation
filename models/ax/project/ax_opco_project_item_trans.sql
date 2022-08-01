{% set _load = load_type('AX_OPCO_PROJECT_ITEM_TRANS') %}

with ax_opco_project_item_trans as (
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
    from {{ source('AX_DEV', 'PROJITEMTRANS') }} pit
    left join {{ ref('ax_opco_project_curr')}} opr
        on pit.dataareaid = opr.opco_id
        and pit.projid = opr.project_id
        and opr.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_curr')}} opco 
        on pit.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_item_curr')}} oi 
        on pit.dataareaid = oi.opco_id
        and pit.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join {{ref('ax_opco_cost_center_curr')}} occ 
        on upper(pit.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ref('ax_opco_dept_curr')}} od 
        on upper(pit.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ref('ax_opco_type_curr')}} ot 
        on upper(pit.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ref('ax_opco_purpose_curr')}} op 
        on upper(pit.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join {{ref('ax_opco_lob_curr')}} ol
        on upper(pit.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_project_catgry_curr')}} opc
        on pit.dataareaid = opc.opco_id
        and pit.categoryid = opc.catgry_cd
        and opc.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_uom_curr')}} ou 
        on upper(pit.salesunit) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_assctn_curr')}} oa 
        on pit.dataareaid = oa.opco_id
        and pit.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_project_trans_status_curr')}} opts
		on pit.transstatus = opts.project_trans_status_cd
		and opts.src_sys_nm = 'AX'
	left join {{ ref('ax_opco_project_trans_origin_curr')}} opto
		on pit.transactionorigin = opto.project_trans_origin_cd
		and opto.src_sys_nm = 'AX'
    where pit.dataareaid not in {{ var('excluded_ax_companies')}}
    and pit.projtransid is not null
    and pit.projtransid <> ''
),
final as (															
    select 
        {{ dbt_utils.surrogate_key(['src_sys_nm','opco_id','project_item_trans_id', 'invtry_trans_id']) }} as opco_project_item_trans_sk,
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'project_item_trans_id', 'invtry_trans_id', 'opco_project_sk', 'opco_sk', 'opco_item_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_project_catgry_sk', 'trans_uom_sk', 'opco_assctn_sk', 'opco_project_trans_status_sk', 'opco_project_trans_origin_sk', 'trans_dt', 'trans_desc', 'line_property_id', 'tax_grp_id', 'tax_item_grp_id', 'packing_slip_voucher_nbr', 'cust_invoice_nbr', 'project_adjsmt_ref_id', 'ref_project_item_trans_id', 'calc_ind', 'project_cost_trans_conversion_ind', 'trans_qty', 'per_unit_sls_price_amt', 'item_price_amt', 'total_revenue_amt', 'total_cost_amt', 'total_std_cost_amt', 'margin_contribution_pct']) }} as opco_project_item_trans_ck,
        concat_ws('|', src_sys_nm, opco_id, project_item_trans_id, invtry_trans_id) as opco_project_item_trans_ak,
        * 		
    from ax_opco_project_item_trans 															
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_PROJECT_ITEM_TRANS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PROJECT_ITEM_TRANS') and _load != 3%}
    union
    select
    OPCO_PROJECT_ITEM_TRANS_sk, OPCO_PROJECT_ITEM_TRANS_ck, OPCO_PROJECT_ITEM_TRANS_ak ,crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, opco_id, project_item_trans_id, invtry_trans_id, opco_project_sk, opco_sk, opco_item_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_project_catgry_sk, trans_uom_sk, opco_assctn_sk, opco_project_trans_status_sk, opco_project_trans_origin_sk, trans_dt, trans_desc, line_property_id, tax_grp_id, tax_item_grp_id, packing_slip_voucher_nbr, cust_invoice_nbr, project_adjsmt_ref_id, ref_project_item_trans_id, calc_ind, project_cost_trans_conversion_ind, trans_qty, per_unit_sls_price_amt, item_price_amt, total_revenue_amt, total_cost_amt, total_std_cost_amt, margin_contribution_pct
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_ITEM_TRANS
    {% if _load == 1 %}
        where OPCO_PROJECT_ITEM_TRANS_ck not in (select distinct OPCO_PROJECT_ITEM_TRANS_ck from final)
    {% else %}
        where OPCO_PROJECT_ITEM_TRANS_ck not in (select distinct OPCO_PROJECT_ITEM_TRANS_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_ITEM_TRANS where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PROJECT_ITEM_TRANS_ck not in (select distinct OPCO_PROJECT_ITEM_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_ITEM_TRANS where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PROJECT_ITEM_TRANS_ck in (select distinct OPCO_PROJECT_ITEM_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_ITEM_TRANS where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}