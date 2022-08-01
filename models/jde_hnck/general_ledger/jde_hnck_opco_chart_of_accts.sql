{% set _load = load_type('JDE_HNCK_OPCO_CHART_OF_ACCTS') %}

with jde_hnck_opco_chart_of_accts as (
    select 
    current_timestamp as crt_dtm,
    null::timestamp_tz as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'JDE-HNCK'::varchar(20) as src_sys_nm,
    iff(trim(f1.gmsub) <> '', concat_ws('.', upper(trim(f1.gmmcu)), trim(f1.gmobj), trim(f1.gmsub)), concat_ws('.', upper(trim(f1.gmmcu)), trim(f1.gmobj)))::varchar(20) as gl_acct_nbr,
    trim(f1.gmco)::varchar(15) as opco_id,
    trim(f1.gmdl01)::varchar(100) as gl_acct_nm,
    null::varchar(32) as chart_of_accts_sk,
    occ.opco_cost_center_sk,
    null::varchar(32) as opco_dept_sk,
    null::varchar(32) as opco_type_sk,
    null::varchar(32) as opco_purpose_sk,
    null::varchar(32) as opco_lob_sk,
    null::varchar(32) as opco_chart_of_accts_type_sk,
    ou.opco_uom_sk,
    null::varchar(120) as gl_acct_alias_nm,
    case 
        when trim(f1.gmpec) = 'M' then 1 
        else 0 
    end::number(1,0) as manual_entry_allowed_ind,
    null::number(1,0) as dpo_exclusion_ind,
    null::number(1,0) as movement_allowed_ind,
    null::varchar(50) as cost_center_reqd_txt,
    null::varchar(50) as dept_reqd_txt,
    null::varchar(50) as type_reqd_txt,
    null::varchar(10) as prnt_gl_acct_nbr,
    null::varchar(10) as related_gl_acct_nbr,
    null::varchar(10) as monetary_gl_acct_ind,
    iff(trim(f1.gmpec) in ('I', 'N'), 0, 1)::number(1,0) as actv_ind,
    iff(trim(f0.mcstyl) = 'BS', 1, 0)::number(1,0) as balance_sheet_acct_ind,
    iff(trim(f1.gmpec) = 'N', 1, 0)::number(1,0) as summary_acct_ind,
    null::varchar(32) as opco_brand_sk,
    upper(trim(f1.gmobj))::varchar(20) as acct_clssfctn_cd,
    upper(trim(f1.gmsub))::varchar(20) as sub_acct_nbr
    from {{source('JDE_DEV','JDE_PRODUCTION_PRODDTA_F0901')}} f1
    left join {{source('JDE_DEV','JDE_PRODUCTION_PRODDTA_F0006')}} f0
        on trim(f1.gmmcu) = trim(f0.mcmcu)
    left join {{ ref('jde_hnck_opco_cost_center_curr')}} occ 
        on upper(trim(f1.gmmcu)) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'JDE-HNCK'
    left join {{ ref('jde_hnck_opco_uom_curr')}} ou 
        on upper(trim(f1.gmum)) = ou.src_uom_cd
        and ou.src_sys_nm = 'JDE-HNCK'
    where gl_acct_nbr is not null
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'gl_acct_nbr']) }} as opco_chart_of_accts_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'gl_acct_nbr',  'opco_id',  'gl_acct_nm',  'chart_of_accts_sk',  'opco_cost_center_sk',  'opco_dept_sk',  'opco_type_sk',  'opco_purpose_sk',  'opco_lob_sk',  'opco_chart_of_accts_type_sk',  'opco_uom_sk',  'gl_acct_alias_nm',  'manual_entry_allowed_ind',  'dpo_exclusion_ind',  'movement_allowed_ind',  'cost_center_reqd_txt',  'dept_reqd_txt',  'type_reqd_txt',  'prnt_gl_acct_nbr',  'related_gl_acct_nbr',  'monetary_gl_acct_ind',  'actv_ind',  'balance_sheet_acct_ind',  'summary_acct_ind',  'opco_brand_sk',  'acct_clssfctn_cd',  'sub_acct_nbr']) }} as opco_chart_of_accts_ck,
            * 
    from jde_hnck_opco_chart_of_accts
),
delete_captured as(
    select * from final    
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'JDE_HNCK_OPCO_CHART_OF_ACCTS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CHART_OF_ACCTS') and _load != 3%}
        union
        select
        OPCO_CHART_OF_ACCTS_sk, OPCO_CHART_OF_ACCTS_ck, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        src_sys_nm, gl_acct_nbr, opco_id, gl_acct_nm, chart_of_accts_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_chart_of_accts_type_sk, opco_uom_sk, gl_acct_alias_nm, manual_entry_allowed_ind, dpo_exclusion_ind, movement_allowed_ind, cost_center_reqd_txt, dept_reqd_txt, type_reqd_txt, prnt_gl_acct_nbr, related_gl_acct_nbr, monetary_gl_acct_ind, actv_ind, balance_sheet_acct_ind, summary_acct_ind, opco_brand_sk, acct_clssfctn_cd, sub_acct_nbr
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS
        {% if _load == 1 %}
            where OPCO_CHART_OF_ACCTS_ck not in (select distinct OPCO_CHART_OF_ACCTS_ck from final)
        {% else %}
            where OPCO_CHART_OF_ACCTS_ck not in (select distinct OPCO_CHART_OF_ACCTS_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS where src_sys_nm = 'JDE-HNCK')
        {% endif %} 
        and src_sys_nm = 'JDE-HNCK'
    {% endif %}
)

select * from delete_captured


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_CHART_OF_ACCTS_ck not in (select distinct OPCO_CHART_OF_ACCTS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS where src_sys_nm = 'JDE-HNCK')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_CHART_OF_ACCTS_ck in (select distinct OPCO_CHART_OF_ACCTS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS where src_sys_nm = 'JDE-HNCK') and delete_dtm is not null)
    {% endif %}
{% endif %}     