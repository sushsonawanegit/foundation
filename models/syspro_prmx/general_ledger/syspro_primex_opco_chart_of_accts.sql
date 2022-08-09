-- depends_on: {{ ref('chart_of_accts') }}
-- depends_on: {{ ref('syspro_primex_opco_cost_center_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_type_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_dept_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_purpose_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_lob_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_chart_of_accts_type_curr') }}

{% set _load = load_type('SYSPRO_PRIMEX_OPCO_CHART_OF_ACCTS') %}

with syspro_primex_opco_chart_of_accts as(
    {% for sch in var('primex_schemas') %}
        {% if tb_check(var('primex_db'), sch, 'GENMASTER') and tb_check(var('primex_db'), sch, 'CUSGENMASTER_') %}
            select
            substr('{{sch}}', 22, 1) as src_comp,
            current_timestamp as crt_dtm,
            cdg.mule_load_ts as stg_load_dtm,
            null::timestamp_tz as delete_dtm,
            'SYSPRO-PRMX' as src_sys_nm,
            upper(trim(cdg.glcode)) as gl_acct_nbr,
            upper(trim(cdg.company)) as opco_id,
            trim(cdg.description) as gl_acct_nm,
            coa.chart_of_accts_sk,
            occ.opco_cost_center_sk,
            od.opco_dept_sk,
            ot.opco_type_sk,
            oip.opco_purpose_sk,
            ol.opco_lob_sk,
            ocoa.opco_chart_of_accts_type_sk,
            null as opco_uom_sk,
            null as gl_acct_alias_nm,
            iff(trim(cdg.controlaccflag) = 'Y', 0, 1) as manual_entry_allowed_ind,
            null as dpo_exclusion_ind,
            null as movement_allowed_ind,
            null as cost_center_reqd_txt,
            null as dept_reqd_txt,
            null as type_reqd_txt,
            null as prnt_gl_acct_nbr,
            null as related_gl_acct_nbr,
            null as monetary_gl_acct_ind,
            iff(trim(cdg.acconhldflag) in ('', 'N'), 1 , 0) as actv_ind,
            iff(trim(cdc.financialstatement) = 'BS', 1, 0) as balance_sheet_acct_ind,
            null as summary_acct_ind,
            null as opco_brand_sk,
            null as acct_clssfctn_cd,
            null as sub_acct_nbr
            from {{ var('primex_db')}}.{{ sch}}.GENMASTER cdg
            left join {{ var('primex_db')}}.{{ sch}}.CUSGENMASTER_ cdc 
                on trim(cdg.glcode) = trim(cdc.glcode)
            left join {{ ref('chart_of_accts')}} coa 
                on coa.gl_acct_nbr = trim(cdc.gl)
            left join {{ ref('syspro_primex_opco_cost_center_curr')}} occ 
                on occ.src_cost_center_cd = upper(trim(cdc.cc))
                and occ.src_sys_nm = 'SYSPRO-PRMX'
            left join {{ ref('syspro_primex_opco_dept_curr')}} od 
                on od.src_dept_cd = upper(trim(cdc.dept))
                and od.src_sys_nm = 'SYSPRO-PRMX'
            left join {{ ref('syspro_primex_opco_type_curr')}} ot 
                on ot.src_type_cd = upper(trim(cdc.type))
                and ot.src_sys_nm = 'SYSPRO-PRMX'
            left join {{ ref('syspro_primex_opco_purpose_curr')}} oip 
                on oip.src_purpose_cd = upper(trim(cdc.purpose))
                and oip.src_sys_nm = 'SYSPRO-PRMX'
            left join {{ ref('syspro_primex_opco_lob_curr')}} ol 
                on ol.src_lob_cd = upper(trim(cdc.fsgroup4buname))
                and ol.src_sys_nm = 'SYSPRO-PRMX'
            left join {{ ref('syspro_primex_opco_chart_of_accts_type_curr')}} ocoa 
                on ocoa.src_acct_type_cd = upper(trim(cdg.glgroup))
                and ocoa.src_sys_nm = 'SYSPRO-PRMX'
            where upper(trim(cdg.company)) = substr('{{sch}}', 22, 1)
            {% if not loop.last %} union all {% endif %}
        {% endif %}
        
    {% endfor %}
),
de_duplication as(
    select *
           , rank() over(partition by opco_id, gl_acct_nbr order by stg_load_dtm, src_comp) as rnk
    from syspro_primex_opco_chart_of_accts
),
final as(
    select distinct
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'gl_acct_nbr']) }} as opco_chart_of_accts_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'gl_acct_nbr', 'opco_id', 'gl_acct_nm', 'chart_of_accts_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_chart_of_accts_type_sk', 'opco_uom_sk', 'GL_ACCT_ALIAS_NM', 'manual_entry_allowed_ind', 'dpo_exclusion_ind', 'movement_allowed_ind', 'cost_center_reqd_txt', 'dept_reqd_txt', 'type_reqd_txt', 'prnt_gl_acct_nbr', 'related_gl_acct_nbr', 'monetary_gl_acct_ind', 'balance_sheet_acct_ind', 'summary_acct_ind', 'OPCO_BRAND_SK', 'acct_clssfctn_cd', 'sub_acct_nbr']) }} as opco_chart_of_accts_ck,
            CRT_DTM, STG_LOAD_DTM, DELETE_DTM, SRC_SYS_NM, GL_ACCT_NBR, OPCO_ID, GL_ACCT_NM, CHART_OF_ACCTS_SK, OPCO_COST_CENTER_SK, OPCO_DEPT_SK, OPCO_TYPE_SK, OPCO_PURPOSE_SK, OPCO_LOB_SK, OPCO_CHART_OF_ACCTS_TYPE_SK, OPCO_UOM_SK, GL_ACCT_ALIAS_NM, MANUAL_ENTRY_ALLOWED_IND, DPO_EXCLUSION_IND, MOVEMENT_ALLOWED_IND, COST_CENTER_REQD_TXT, DEPT_REQD_TXT, TYPE_REQD_TXT, PRNT_GL_ACCT_NBR, RELATED_GL_ACCT_NBR, MONETARY_GL_ACCT_IND, ACTV_IND, BALANCE_SHEET_ACCT_IND, SUMMARY_ACCT_IND, OPCO_BRAND_SK, ACCT_CLSSFCTN_CD, SUB_ACCT_NBR
    from de_duplication
    where rnk = 1 
),
delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'SYSPRO_PRIMEX_OPCO_CHART_OF_ACCTS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CHART_OF_ACCTS') and _load != 3%}
        union
        select
        OPCO_CHART_OF_ACCTS_sk, OPCO_CHART_OF_ACCTS_ck, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        SRC_SYS_NM, GL_ACCT_NBR, OPCO_ID, GL_ACCT_NM, CHART_OF_ACCTS_SK, OPCO_COST_CENTER_SK, OPCO_DEPT_SK, OPCO_TYPE_SK, OPCO_PURPOSE_SK, OPCO_LOB_SK, OPCO_CHART_OF_ACCTS_TYPE_SK, OPCO_UOM_SK, GL_ACCT_ALIAS_NM, MANUAL_ENTRY_ALLOWED_IND, DPO_EXCLUSION_IND, MOVEMENT_ALLOWED_IND, COST_CENTER_REQD_TXT, DEPT_REQD_TXT, TYPE_REQD_TXT, PRNT_GL_ACCT_NBR, RELATED_GL_ACCT_NBR, MONETARY_GL_ACCT_IND, ACTV_IND, BALANCE_SHEET_ACCT_IND, SUMMARY_ACCT_IND, OPCO_BRAND_SK, ACCT_CLSSFCTN_CD, SUB_ACCT_NBR
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS
        {% if _load == 1 %}
            where OPCO_CHART_OF_ACCTS_ck not in (select distinct OPCO_CHART_OF_ACCTS_ck from final)
        {% else %}
            where OPCO_CHART_OF_ACCTS_ck not in (select distinct OPCO_CHART_OF_ACCTS_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS where src_sys_nm = 'SYSPRO-PRMX')
        {% endif %} 
        and src_sys_nm = 'SYSPRO-PRMX'
    {% endif %}
)

select * from delete_captured


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_CHART_OF_ACCTS_ck not in (select distinct OPCO_CHART_OF_ACCTS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_CHART_OF_ACCTS_ck in (select distinct OPCO_CHART_OF_ACCTS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    {% endif %}
{% endif %}