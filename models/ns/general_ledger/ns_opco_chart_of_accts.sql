{% set _load = load_type('NS_OPCO_CHART_OF_ACCTS') %}

with ns_opco_chart_of_accts as (
    select 
    current_timestamp as crt_dtm,
    acct._fivetran_synced as stg_load_dtm,
    case
        when acct._fivetran_deleted = true then acct._fivetran_synced
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    acct.accountnumber::varchar(20) as gl_acct_nbr,
    null::varchar(15) as opco_id,
    acct.name::varchar(100) as gl_acct_nm,
    coa.chart_of_accts_sk,
    cc.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    null::varchar(32) as opco_purpose_sk,
    null::varchar(32) as opco_lob_sk,
    cat.opco_chart_of_accts_type_sk,
    ou.opco_uom_sk,
    acct.full_name::varchar(120) as gl_acct_alias_nm,
    null::number(1,0) as manual_entry_allowed_ind,
    case 
        when acct.dpo_flag = 'T' then 0::number(1,0)
        when acct.dpo_flag = 'F' then 1::number(1,0)
    end as dpo_exclusion_ind,
    null::number(1,0) as movement_allowed_ind,
    null::varchar(50) as cost_center_reqd_txt,
    null::varchar(50) as dept_reqd_txt,
    null::varchar(50) as type_reqd_txt,
    acct.parent_id::varchar(10) as prnt_gl_acct_nbr,
    null::varchar(10) as related_gl_acct_nbr,
    null::varchar(10) as monetary_gl_acct_ind,
    iff(acct.isinactive='No', 1::number(1,0), 0::number(1,0)) as actv_ind,
    case
        when is_balancesheet = 'T' then 1::number(1,0)
        when is_balancesheet = 'F' then 0::number(1,0)
    end as balance_sheet_acct_ind,
    case
        when is_summary = 'Yes' then 1::number(1,0)
        when is_summary = 'No' then 0::number(1,0)
    end as summary_acct_ind,
    ob.opco_brand_sk,
    null::varchar(20) as acct_clssfctn_cd,
    null::varchar(20) as sub_acct_nbr
    from {{source('NS_DEV', 'ACCOUNTS')}} acct
    left join {{ref('chart_of_accts')}} coa 
        on left(acct.opi_ax_acct, 6) = coa.gl_acct_nbr
        and acct.opi_ax_acct is not null
        and acct.opi_ax_acct not in ('none', 'None')
    left join {{ref('ns_opco_cost_center_curr')}} cc 
        on upper(acct.location_id) = cc.src_cost_center_cd
        and cc.src_sys_nm = 'NS'
    left join {{ref('ns_opco_dept_curr')}} od 
        on upper(acct.department_id) = od.src_dept_cd
        and od.src_sys_nm = 'NS'
    left join {{ref('ns_opco_type_curr')}} ot 
        on upper(substr(acct.opi_ax_acct, 8)) = ot.src_type_cd
        and ot.src_sys_nm = 'NS'
        and acct.opi_ax_acct is not null
    left join {{ref('ns_opco_chart_of_accts_type_curr')}} cat 
        on upper(acct.type_name) = cat.src_acct_type_cd
        and cat.src_sys_nm = 'NS'
    left join {{ref('ns_opco_uom_curr')}} ou 
        on upper(acct.opi_qty_uom_id) = ou.src_uom_cd
        and ou.src_sys_nm = 'NS'
    left join {{ ref('ns_opco_brand_curr')}} ob 
        on acct.class_id = ob.src_brand_cd
        and ob.src_sys_nm = 'NS'
    where acct.accountnumber is not null
),
final as (
    select  {{ dbt_utils.surrogate_key(['cat.src_sys_nm', 'cat.gl_acct_nbr', 'sb.subsidiary_id']) }} as opco_chart_of_accts_sk,
            {{ dbt_utils.surrogate_key(['cat.src_sys_nm', 'cat.gl_acct_nbr', 'sb.subsidiary_id', 'cat.gl_acct_nm', 'cat.chart_of_accts_sk', 'cat.opco_cost_center_sk', 'cat.opco_dept_sk', 'cat.opco_type_sk', 'cat.opco_purpose_sk', 'cat.opco_lob_sk', 'cat.opco_chart_of_accts_type_sk', 'cat.opco_uom_sk', 'cat.gl_acct_alias_nm', 'cat.manual_entry_allowed_ind', 'cat.dpo_exclusion_ind', 'cat.movement_allowed_ind', 'cat.cost_center_reqd_txt', 'cat.dept_reqd_txt', 'cat.type_reqd_txt', 'accts.accountnumber', 'cat.related_gl_acct_nbr', 'cat.monetary_gl_acct_ind', 'cat.actv_ind', 'cat.balance_sheet_acct_ind', 'cat.summary_acct_ind', 'cat.opco_brand_sk', 'cat.acct_clssfctn_cd', 'cat.sub_acct_nbr']) }} as opco_chart_of_accts_ck,
            cat.crt_dtm, cat.stg_load_dtm, cat.delete_dtm, cat.src_sys_nm, cat.gl_acct_nbr, sb.subsidiary_id as opco_id, cat.gl_acct_nm, cat.chart_of_accts_sk, cat.opco_cost_center_sk, cat.opco_dept_sk, cat.opco_type_sk, cat.opco_purpose_sk, cat.opco_lob_sk, cat.opco_chart_of_accts_type_sk, cat.opco_uom_sk, cat.gl_acct_alias_nm, cat.manual_entry_allowed_ind, cat.dpo_exclusion_ind, cat.movement_allowed_ind, cat.cost_center_reqd_txt, cat.dept_reqd_txt, cat.type_reqd_txt, accts.accountnumber as prnt_gl_acct_nbr, cat.related_gl_acct_nbr, cat.monetary_gl_acct_ind, cat.actv_ind, cat.balance_sheet_acct_ind, cat.summary_acct_ind, cat.opco_brand_sk, cat.acct_clssfctn_cd, cat.sub_acct_nbr
    from ns_opco_chart_of_accts cat
    left join {{ source('NS_DEV', 'ACCOUNTS')}} accts 
        on accts.account_id = cat.prnt_gl_acct_nbr
    cross join {{ source('NS_DEV', 'SUBSIDIARIES') }} sb 
        where sb.subsidiary_id not in (1, 4, 5, 7)
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'NS_OPCO_CHART_OF_ACCTS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CHART_OF_ACCTS') and _load != 3%}
    union
    select
    OPCO_CHART_OF_ACCTS_SK, OPCO_CHART_OF_ACCTS_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, gl_acct_nbr, opco_id, gl_acct_nm, chart_of_accts_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_chart_of_accts_type_sk, opco_uom_sk, gl_acct_alias_nm, manual_entry_allowed_ind, dpo_exclusion_ind, movement_allowed_ind, cost_center_reqd_txt, dept_reqd_txt, type_reqd_txt, prnt_gl_acct_nbr, related_gl_acct_nbr, monetary_gl_acct_ind, actv_ind, balance_sheet_acct_ind, summary_acct_ind, opco_brand_sk, acct_clssfctn_cd, sub_acct_nbr
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS
    {% if _load == 1 %}
        where OPCO_CHART_OF_ACCTS_ck not in (select distinct OPCO_CHART_OF_ACCTS_ck from final)
    {% else %}
        where OPCO_CHART_OF_ACCTS_ck not in (select distinct OPCO_CHART_OF_ACCTS_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS where src_sys_nm = 'NS')
    {% endif %} 
    and src_sys_nm = 'NS'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_CHART_OF_ACCTS_ck not in (select distinct OPCO_CHART_OF_ACCTS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS where src_sys_nm = 'NS')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_CHART_OF_ACCTS_ck in (select distinct OPCO_CHART_OF_ACCTS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS where src_sys_nm = 'NS') and delete_dtm is not null)
    {% endif %}
{% endif %}