{% set _load = load_type('AX_OPCO_CHART_OF_ACCTS') %}

with ax_opco_chart_of_accts as (
    select 
    current_timestamp as crt_dtm,
    lt._fivetran_synced as stg_load_dtm,
    case
        when lt._fivetran_deleted = true then lt._fivetran_synced
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    lt.accountnum::varchar(20) as gl_acct_nbr,
    lt.dataareaid::varchar(15) as opco_id,
    lt.accountname::varchar(100) as gl_acct_nm,
    coa.chart_of_accts_sk,
    cc.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    oip.opco_purpose_sk,
    ol.opco_lob_sk,
    cat.opco_chart_of_accts_type_sk,
    ou.opco_uom_sk,
    lt.accountnamealias::varchar(120) as gl_acct_alias_nm,
    case 
        when lt.blockedinjournal = 0 then 1
        when lt.blockedinjournal = 1 then 0 
    end::numeric(1,0) as manual_entry_allowed_ind,
    lt.excludefromdpo_opi::numeric(1,0) as dpo_exclusion_ind,
    lt.movementjournal_opi::numeric(1,0) as movement_allowed_ind,
    case 
        when lt.mandatorydimension = 0 then 'Optional'
        when lt.mandatorydimension = 1 then 'To Be Filled In'
        when lt.mandatorydimension = 2 then 'Table'
        when lt.mandatorydimension = 3 then 'List'
        when lt.mandatorydimension = 4 then 'Fixed'
        when lt.mandatorydimension = 5 then 'Default'
    end::varchar(50) as cost_center_reqd_txt,
    case 
        when lt.mandatorydimension2_ = 0 then 'Optional'
        when lt.mandatorydimension2_ = 1 then 'To Be Filled In'
        when lt.mandatorydimension2_ = 2 then 'Table'
        when lt.mandatorydimension2_ = 3 then 'List'
        when lt.mandatorydimension2_ = 4 then 'Fixed'
        when lt.mandatorydimension2_ = 5 then 'Default'
    end::varchar(50) as dept_reqd_txt,
    case 
        when lt.mandatorydimension3_ = 0 then 'Optional'
        when lt.mandatorydimension3_ = 1 then 'To Be Filled In'
        when lt.mandatorydimension3_ = 2 then 'Table'
        when lt.mandatorydimension3_ = 3 then 'List'
        when lt.mandatorydimension3_ = 4 then 'Fixed'
        when lt.mandatorydimension3_ = 5 then 'Default'
    end::varchar(50) as type_reqd_txt,
    lt.parentaccount_opi::varchar(10) as prnt_gl_acct_nbr,
    lt.relatedaccount_opi::varchar(10) as related_gl_acct_nbr,
    lt.monetary::varchar(10) as monetary_gl_acct_ind,
    iff(lt.closed=0, 1,0)::numeric(1,0) as actv_ind,
    case
        when accountpltype = 3 then 1
        else 0
    end::numeric(1,0) as balance_sheet_acct_ind,
    case
        when accountpltype = 9 then 1
        else 0
    end::numeric(1,0) as summary_acct_ind,
    null::varchar(32) as opco_brand_sk,
    null::varchar(20) as acct_clssfctn_cd,
    null::varchar(20) as sub_acct_nbr
    from {{source('AX_DEV', 'LEDGERTABLE')}} lt
    left join {{ref('chart_of_accts')}} coa 
        on lt.accountnum = coa.gl_acct_nbr
    left join {{ref('opco_cost_center')}} cc 
        on upper(lt.dimension) = cc.src_cost_center_cd
        and cc.src_sys_nm = 'AX'
    left join {{ref('opco_dept')}} od 
        on upper(lt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ref('opco_type')}} ot 
        on upper(lt.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ref('opco_purpose')}} oip
        on upper(lt.dimension4_) = oip.src_purpose_cd
        and oip.src_sys_nm = 'AX'
    left join {{ref('opco_lob')}} ol 
        on upper(lt.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ref('opco_chart_of_accts_type')}} cat 
        on cast(lt.accountpltype as varchar) = cat.src_acct_type_cd
        and cat.src_sys_nm = 'AX'
    left join {{ref('opco_uom')}} ou 
        on upper(lt.unitid_opi) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    where upper(lt.dataareaid) != 'UGL' 
    and lt.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'gl_acct_nbr']) }} as opco_chart_of_accts_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'gl_acct_nbr', 'opco_id', 'gl_acct_nm', 'chart_of_accts_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_chart_of_accts_type_sk', 'opco_uom_sk', 'gl_acct_alias_nm', 'manual_entry_allowed_ind', 'dpo_exclusion_ind', 'movement_allowed_ind', 'cost_center_reqd_txt', 'dept_reqd_txt', 'type_reqd_txt', 'prnt_gl_acct_nbr', 'related_gl_acct_nbr', 'monetary_gl_acct_ind', 'actv_ind', 'balance_sheet_acct_ind', 'summary_acct_ind', 'opco_brand_sk', 'acct_clssfctn_cd', 'sub_acct_nbr']) }} as opco_chart_of_accts_ck,
            * 
    from ax_opco_chart_of_accts
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_CHART_OF_ACCTS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CHART_OF_ACCTS') and _load != 3%}
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
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS where src_sys_nm = 'AX')
        {% endif %} 
        and src_sys_nm = 'AX'
    {% endif %}
)
select * from final


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_CHART_OF_ACCTS_ck not in (select distinct OPCO_CHART_OF_ACCTS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_CHART_OF_ACCTS_ck in (select distinct OPCO_CHART_OF_ACCTS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}