{% set _load = load_type('AX_OPCO_PROJECT_EMPL_TRANS') %}

with ax_opco_project_empl_trans as (
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
    from {{ source('AX_DEV', 'PROJEMPLTRANS') }} pet
    left join {{ ref('opco_project')}} opr
        on pet.dataareaid = opr.opco_id
        and pet.projid = opr.project_id
        and opr.src_sys_nm = 'AX'
    left join {{ ref('opco')}} opco 
        on pet.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ref('opco_cost_center')}} occ 
        on upper(pet.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ref('opco_dept')}} od 
        on upper(pet.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ref('opco_type')}} ot 
        on upper(pet.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ref('opco_purpose')}} op 
        on upper(pet.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join {{ref('opco_lob')}} ol
        on upper(pet.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('opco_project_catgry')}} opc
        on pet.dataareaid = opc.opco_id
        and pet.categoryid = opc.catgry_cd
        and opc.src_sys_nm = 'AX'
    left join {{ ref('opco_project_trans_status')}} opts
		on pet.transstatus = opts.project_trans_status_cd
		and opts.src_sys_nm = 'AX'
	left join {{ ref('opco_project_trans_origin')}} opto
		on pet.transactionorigin = opto.project_trans_origin_cd
		and opto.src_sys_nm = 'AX'
    left join {{ ref('opco_currency')}} ocr
        on upper(pet.currencyid) = ocr.src_currency_cd
        and ocr.src_sys_nm = 'AX'
    where pet.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (															
    select 
        {{ dbt_utils.surrogate_key(['src_sys_nm','opco_id','project_empl_trans_id']) }} as opco_project_empl_trans_sk,
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'project_empl_trans_id', 'opco_project_sk', 'opco_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_project_catgry_sk', 'opco_project_trans_status_sk', 'opco_project_trans_origin_sk', 'trans_currency_sk', 'empl_id', 'line_property_id', 'ref_project_empl_trans_id', 'tax_grp_id', 'trans_dt', 'gl_voucher_nbr', 'gl_section_txt', 'project_empl_trans_desc', 'calc_ind', 'trans_hour_qty', 'opco_currency_hrly_cost_price_amt', 'trans_currency_hrly_sls_price_amt', 'item_price_amt', 'total_revenue_amt', 'total_cost_amt', 'total_std_cost_amt', 'margin_contribution_pct']) }} as opco_project_empl_trans_ck,
        concat_ws('|', src_sys_nm, opco_id, project_empl_trans_id) as opco_project_empl_trans_ak,
        * 		
    from ax_opco_project_empl_trans opet															
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_PROJECT_EMPL_TRANS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PROJECT_EMPL_TRANS') and _load != 3%}
    union
    select
    OPCO_PROJECT_EMPL_TRANS_sk, OPCO_PROJECT_EMPL_TRANS_ck, OPCO_PROJECT_EMPL_TRANS_ak, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, opco_id, project_empl_trans_id, opco_project_sk, opco_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_project_catgry_sk, opco_project_trans_status_sk, opco_project_trans_origin_sk, trans_currency_sk, empl_id, line_property_id, ref_project_empl_trans_id, tax_grp_id, trans_dt, gl_voucher_nbr, gl_section_txt, project_empl_trans_desc, calc_ind, trans_hour_qty, opco_currency_hrly_cost_price_amt , trans_currency_hrly_sls_price_amt, item_price_amt, total_revenue_amt, total_cost_amt, total_std_cost_amt, margin_contribution_pct
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_EMPL_TRANS
    {% if _load == 1 %}
        where OPCO_PROJECT_EMPL_TRANS_ck not in (select distinct OPCO_PROJECT_EMPL_TRANS_ck from final)
    {% else %}
        where OPCO_PROJECT_EMPL_TRANS_ck not in (select distinct OPCO_PROJECT_EMPL_TRANS_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_EMPL_TRANS where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PROJECT_EMPL_TRANS_ck not in (select distinct OPCO_PROJECT_EMPL_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_EMPL_TRANS where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PROJECT_EMPL_TRANS_ck in (select distinct OPCO_PROJECT_EMPL_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_EMPL_TRANS where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}