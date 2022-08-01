{% set _load = load_type('AX_OPCO_PROJECT_EMPL_FRCST') %}

with ax_opco_project_empl_frcst as (
    select
    current_timestamp as crt_dtm,
    pfc._fivetran_synced as stg_load_dtm,
    case
        when pfc._fivetran_deleted = true then pfc._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    pfc.dataareaid as opco_id,
    pfc.transid as project_empl_frcst_id,
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
    pfc.linenum as line_nbr,
    pfc.linepropertyid as line_property_id,
    pfc.emplid as empl_id,
    pfc.modelid as model_id,
    pfc.taxgroupid as tax_grp_id,
    pfc.active as include_amt_ind,
    pfc.islock_opi as lock_ind,
    pfc.schedlinktype as sched_link_type_txt,
    pfc.schedlink as sched_link_txt,
    pfc.schedfromdate as sched_from_dt,
    pfc.schedworktime as sched_worktime_ind,
    pfc.schedcapacity as sched_capacity_ind,
    pfc.salespaymdate as revenue_pymnt_expctd_dt,
    pfc.costpaymdate as cost_pymnt_expctd_dt,
    pfc.invoicedate as invoice_expctd_dt,
    pfc.eliminationdate as eliminatin_expctd_dt,
    pfc.qty as trans_hours_qty,
    pfc.schedloadpct as sched_load_pct,
    pfc.costprice as trans_currency_cost_price_amt,
    pfc.salesprice as trans_currency_sls_price_amt,
    pfc.totalrevenue_opi as total_revenue_amt,
    pfc.totalcost_opi as total_cost_amt,
    pfc.mcpercent_opi as margin_contribution_pct
    from {{ source('AX_DEV', 'PROJFORECASTEMPL') }} pfc
    left join {{ ref('ax_opco_project_curr')}} opr
        on pfc.dataareaid = opr.opco_id
        and pfc.projid = opr.project_id
        and opr.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_curr')}} opco 
        on pfc.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ref('ax_opco_cost_center_curr')}} occ 
        on upper(pfc.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ref('ax_opco_dept_curr')}} od 
        on upper(pfc.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ref('ax_opco_type_curr')}} ot 
        on upper(pfc.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ref('ax_opco_purpose_curr')}} op 
        on upper(pfc.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join {{ref('ax_opco_lob_curr')}} ol
        on upper(pfc.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_project_catgry_curr')}} opc
        on pfc.dataareaid = opc.opco_id
        and pfc.categoryid = opc.catgry_cd
        and opc.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_currency_curr')}} ocr
        on upper(pfc.currencyid) = ocr.src_currency_cd
        and ocr.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_uom_curr')}} ou 
        on upper(pfc.unitid_opi) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    where pfc.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select 
        {{ dbt_utils.surrogate_key(['src_sys_nm','opco_id','project_empl_frcst_id']) }} as opco_project_empl_frcst_sk,
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'project_empl_frcst_id', 'opco_project_sk', 'opco_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_project_catgry_sk', 'trans_currency_sk', 'opco_uom_sk', 'line_nbr', 'line_property_id', 'empl_id', 'model_id', 'tax_grp_id', 'include_amt_ind', 'lock_ind', 'sched_link_type_txt', 'sched_link_txt', 'sched_from_dt', 'sched_worktime_ind', 'sched_capacity_ind', 'revenue_pymnt_expctd_dt', 'cost_pymnt_expctd_dt', 'invoice_expctd_dt', 'eliminatin_expctd_dt', 'trans_hours_qty', 'sched_load_pct', 'trans_currency_cost_price_amt', 'trans_currency_sls_price_amt', 'total_revenue_amt', 'total_cost_amt', 'margin_contribution_pct']) }} as opco_project_empl_frcst_ck,
        concat_ws('|', src_sys_nm, opco_id, project_empl_frcst_id) as opco_project_empl_frcst_ak,
        * 		
        from ax_opco_project_empl_frcst															
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_PROJECT_EMPL_FRCST') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PROJECT_EMPL_FRCST') and _load != 3%}
    union
    select
    OPCO_PROJECT_EMPL_FRCST_sk, OPCO_PROJECT_EMPL_FRCST_ck, OPCO_PROJECT_EMPL_FRCST_ak, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, opco_id, project_empl_frcst_id, opco_project_sk, opco_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_project_catgry_sk, trans_currency_sk, opco_uom_sk, line_nbr, line_property_id, empl_id, model_id, tax_grp_id, include_amt_ind, lock_ind, sched_link_type_txt, sched_link_txt, sched_from_dt, sched_worktime_ind, sched_capacity_ind, revenue_pymnt_expctd_dt, cost_pymnt_expctd_dt, invoice_expctd_dt, eliminatin_expctd_dt, trans_hours_qty, sched_load_pct, trans_currency_cost_price_amt, trans_currency_sls_price_amt, total_revenue_amt, total_cost_amt, margin_contribution_pct
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_EMPL_FRCST
    {% if _load == 1 %}
        where OPCO_PROJECT_EMPL_FRCST_ck not in (select distinct OPCO_PROJECT_EMPL_FRCST_ck from final)
    {% else %}
        where OPCO_PROJECT_EMPL_FRCST_ck not in (select distinct OPCO_PROJECT_EMPL_FRCST_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_EMPL_FRCST where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PROJECT_EMPL_FRCST_ck not in (select distinct OPCO_PROJECT_EMPL_FRCST_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_EMPL_FRCST where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PROJECT_EMPL_FRCST_ck in (select distinct OPCO_PROJECT_EMPL_FRCST_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_EMPL_FRCST where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}