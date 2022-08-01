-- depends_on: {{ ref('syspro_primex_opco_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_chart_of_accts_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_cost_center_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_type_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_dept_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_purpose_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_lob_curr') }}
-- depends_on: {{ ref('fscl_clndr') }}

{% set _load = load_type('SYSPRO_PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL') %}

with syspro_primex_opco_fscl_yr_gl_opening_bal as (
   {% set tables = table_check('COMPANY', '_DBO_GENHISTORY') %}
   {% for tbl in tables %}
      select 
      '{{tbl}}' as src_comp,
      current_timestamp as crt_dtm,
      null::timestamp_tz as stg_load_dtm,
      null::timestamp_tz as delete_dtm,
      {{ dbt_utils.surrogate_key(["'SYSPRO-PRMX'", "ifnull(trim(gh.company), '{{tbl}}')", 'trim(gh.glyear)', 'trim(gh.glcode)']) }} as opco_fscl_yr_gl_opening_bal_sk, 
      concat_ws('|', 'SYSPRO-PRMX', ifnull(trim(gh.company), '{{tbl}}'), trim(gh.glyear), trim(gh.glcode)) as opco_fscl_yr_gl_opening_bal_ak,
      'SYSPRO-PRMX' as src_sys_nm,
      null as src_key_txt,
      opco.opco_sk,
      ocoa.opco_chart_of_accts_sk,
      null as opco_gl_trans_type_sk,
      null as opco_gl_trans_posting_type_sk,
      occ.opco_cost_center_sk,
      od.opco_dept_sk,
      ot.opco_type_sk,
      op.opco_purpose_sk,
      ol.opco_lob_sk,
      opco.opco_currency_sk as trans_currency_sk,
      case 
         when trim(gh.glyear) >= 2020 then fc.fscl_yr_strt_dt
         when trim(gh.glyear) < 2020 then concat(trim(gh.glyear), '-04-01')::date
      end as fscl_yr_strt_dt,
      trim(gh.glyear) as fscl_yr_nbr,
      null as credit_ind,
      null as opening_bal_qty,
      trim(gh.beginyearbalance) as trans_currency_opening_bal_amt,
      trim(gh.beginyearbalance) as opco_currency_opening_bal_amt,
      null as opco_uom_sk,
      null as opco_brand_sk,
      null as opco_sub_ledger_type_sk,
      null as sub_ledger_cd
      from {{var('primex_db')}}.{{var('primex_schema')}}.COMPANY{{tbl}}_DBO_GENHISTORY as gh
      left join  {{var('primex_db')}}.{{var('primex_schema')}}.COMPANY{{tbl}}_DBO_CUSGENMASTER_ as cgm
         on gh.glcode=cgm.glcode
      left join {{ref('syspro_primex_opco_curr')}} opco 
         on opco.opco_id = iff(trim(gh.company) is null, '{{ tbl}}', trim(gh.company))
         and opco.src_sys_nm = 'SYSPRO-PRMX'
      left join {{ref('syspro_primex_opco_chart_of_accts_curr')}} ocoa 
         on ocoa.gl_acct_nbr = trim(gh.glcode)
         and ocoa.src_sys_nm = 'SYSPRO-PRMX'
      left join {{ref('syspro_primex_opco_cost_center_curr')}} occ
         on occ.src_cost_center_cd = upper(trim(cgm.cc)) 
         and occ.src_sys_nm = 'SYSPRO-PRMX'
      left join {{ref('syspro_primex_opco_dept_curr')}} od 
         on od.src_dept_cd = upper(trim(cgm.dept)) 
         and od.src_sys_nm = 'SYSPRO-PRMX'
      left join {{ref('syspro_primex_opco_type_curr')}} ot 
         on ot.src_type_cd = upper(trim(cgm.type)) 
         and ot.src_sys_nm = 'SYSPRO-PRMX'
      left join {{ref('syspro_primex_opco_purpose_curr')}} op 
         on op.src_purpose_cd = upper(trim(cgm.purpose)) 
         and op.src_sys_nm = 'SYSPRO-PRMX'
      left join {{ref('syspro_primex_opco_lob_curr')}} ol 
         on ol.src_lob_cd = upper(trim(cgm.fsgroup4buname)) 
         and ol.src_sys_nm = 'SYSPRO-PRMX' 
      left join {{ ref('fscl_clndr')}} fc 
         on fc.fscl_yr_id = trim(gh.glyear)
      where trim(gh.glyear) >= 2020 and trim(gh.company) in (null, '{{ tbl}}')
      {% if not loop.last %} union all {% endif %}
   {% endfor %}  
),
de_duplication as(
    select *
           , rank() over(partition by opco_fscl_yr_gl_opening_bal_ak order by stg_load_dtm, src_comp) as rnk
    from syspro_primex_opco_fscl_yr_gl_opening_bal
),

final as(
    select  distinct 
            opco_fscl_yr_gl_opening_bal_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_key_txt', 'opco_sk', 'opco_chart_of_accts_sk', 'opco_gl_trans_type_sk', 'opco_gl_trans_posting_type_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'trans_currency_sk', 'fscl_yr_strt_dt', 'fscl_yr_nbr', 'credit_ind', 'opening_bal_qty', 'trans_currency_opening_bal_amt', 'opco_currency_opening_bal_amt', 'opco_uom_sk', 'opco_brand_sk', 'opco_sub_ledger_type_sk', 'sub_ledger_cd'])}} as OPCO_FSCL_YR_GL_OPENING_BAL_CK,
            opco_fscl_yr_gl_opening_bal_ak,
            CRT_DTM, STG_LOAD_DTM, DELETE_DTM, SRC_SYS_NM, SRC_KEY_TXT, OPCO_SK, OPCO_CHART_OF_ACCTS_SK, OPCO_GL_TRANS_TYPE_SK, OPCO_GL_TRANS_POSTING_TYPE_SK, OPCO_COST_CENTER_SK, OPCO_DEPT_SK, OPCO_TYPE_SK, OPCO_PURPOSE_SK, OPCO_LOB_SK, TRANS_CURRENCY_SK, FSCL_YR_STRT_DT, FSCL_YR_NBR, CREDIT_IND, OPENING_BAL_QTY, TRANS_CURRENCY_OPENING_BAL_AMT, OPCO_CURRENCY_OPENING_BAL_AMT, OPCO_UOM_SK, OPCO_BRAND_SK, OPCO_SUB_LEDGER_TYPE_SK, SUB_LEDGER_CD
    from de_duplication
    where rnk = 1 
),

delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'SYSPRO_PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_FSCL_YR_GL_OPENING_BAL') and _load != 3%}
        union
        select
        OPCO_FSCL_YR_GL_OPENING_BAL_sk, OPCO_FSCL_YR_GL_OPENING_BAL_ck, OPCO_FSCL_YR_GL_OPENING_BAL_ak, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        SRC_SYS_NM, SRC_KEY_TXT, OPCO_SK, OPCO_CHART_OF_ACCTS_SK, OPCO_GL_TRANS_TYPE_SK, OPCO_GL_TRANS_POSTING_TYPE_SK, OPCO_COST_CENTER_SK, OPCO_DEPT_SK, OPCO_TYPE_SK, OPCO_PURPOSE_SK, OPCO_LOB_SK, TRANS_CURRENCY_SK, FSCL_YR_STRT_DT, FSCL_YR_NBR, CREDIT_IND, OPENING_BAL_QTY, TRANS_CURRENCY_OPENING_BAL_AMT, OPCO_CURRENCY_OPENING_BAL_AMT, OPCO_UOM_SK, OPCO_BRAND_SK, OPCO_SUB_LEDGER_TYPE_SK, SUB_LEDGER_CD     
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL
        {% if _load == 1 %}
            where OPCO_FSCL_YR_GL_OPENING_BAL_ck not in (select distinct OPCO_FSCL_YR_GL_OPENING_BAL_ck from final)
        {% else %}
            where OPCO_FSCL_YR_GL_OPENING_BAL_ck not in (select distinct OPCO_FSCL_YR_GL_OPENING_BAL_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL where src_sys_nm = 'SYSPRO-PRMX')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_FSCL_YR_GL_OPENING_BAL_ck not in (select distinct OPCO_FSCL_YR_GL_OPENING_BAL_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_FSCL_YR_GL_OPENING_BAL_ck in (select distinct OPCO_FSCL_YR_GL_OPENING_BAL_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    {% endif %}
{% endif %}