{{
    config(
        materialized='table',
        transient=false
    )
}}

{% set _load = load_type('NS_OPCO_FSCL_YR_GL_OPENING_BAL') %}

with ns_opco_fscl_yr_gl_opening_bal as(
    select 
    'NS'::varchar(20) as src_sys_nm,
    null::varchar(50) as src_key_txt,
    ifnull(tl.subsidiary_id, -1) as subsidiary_id,
    ifnull(a.accountnumber, '') as accountnumber,
    ifnull(t.transaction_type, '') as transaction_type,
    null as opco_gl_trans_posting_type_sk,
    max(case 
            when concat_ws('|', tl.transaction_id, tl.transaction_line_id) in ('28794802|0', '28794802|1', '28794152|0', '28794152|1', '28794477|1', '28794477|0', '29073638|0', '29073638|1', '29072175|0', '29072175|1') then '4'
            when concat_ws('|', tl.transaction_id, tl.transaction_line_id) in ('261322|0', '261322|12', '261322|13') then '12'
            else ifnull(tl.location_id, -1) 
        end) as location_id,
    ifnull(tl.department_id, -1) as department_id,
    ifnull(upper(substr(a.opi_ax_acct, 8)), '') as type,
    ifnull(tl.market_opi_id, -1) as market_opi_id,
    ifnull(tl.class_id, -1) as class_id,
    ifnull(t.currency_id, -1) as currency_id,
    ifnull(fscl_yr_id, -1) as fscl_yr_nbr,
    max(a.is_leftside) as credit_ind,
    sum(ifnull(tl.opi_qty, 0)) as opening_bal_qty,
    sum(ifnull(tl.amount_foreign, 0)) as trans_currency_opening_bal_amt,
    sum(ifnull(tl.amount, 0)) as opco_currency_opening_bal_amt
    from {{ source('NS_DEV', 'TRANSACTION_LINES') }} tl
    left join {{ source('NS_DEV', 'TRANSACTIONS')}} t 
        on tl.transaction_id = t.transaction_id
        and t._fivetran_deleted = false 
    left join {{ source('NS_DEV', 'ACCOUNTING_PERIODS')}} ap 
        on t.accounting_period_id = ap.accounting_period_id
        and ap._fivetran_deleted = false 
    left join {{ source('NS_DEV', 'ACCOUNTS')}} a
        on tl.account_id = a.account_id
        and a._fivetran_deleted = false 
    left join {{ ref('fscl_clndr')}} fc
        on fc.clndr_dt = date(ap.ending)
    where tl.subsidiary_id in (2, 3, 8, 9)
    and case subsidiary_id
            when 2 then year(fc.fscl_yr_strt_dt) >= '2006'
            when 3 then year(fc.fscl_yr_strt_dt) >= '2006'
            when 8 then year(fc.fscl_yr_strt_dt) >= '2016'
            when 9 then year(fc.fscl_yr_strt_dt) >= '2017'
        end
    and tl.account_id is not null
    and tl.non_posting_line = 'No'
    and fc.fscl_yr_id < year(current_timestamp)
    group by subsidiary_id, accountnumber, transaction_type, tl.location_id, tl.department_id, type, tl.market_opi_id, tl.class_id, t.currency_id, fscl_yr_id
),
ALL_LEDGER_COMBINATIONS AS
(
    SELECT DISTINCT * FROM
    (SELECT DISTINCT src_sys_nm, subsidiary_id, accountnumber, transaction_type, location_id, department_id, type, market_opi_id, class_id, currency_id
    FROM ns_opco_fscl_yr_gl_opening_bal) A
    CROSS JOIN 
    (SELECT DISTINCT FSCL_YR_NBR FROM ns_opco_fscl_yr_gl_opening_bal B)
),
rolled_up_balance as(
    select *,
        sum(ifnull(opening_bal_qty, 0)) over(partition by subsidiary_id, accountnumber, transaction_type, location_id, department_id, type, market_opi_id, class_id, currency_id order by fscl_yr_nbr asc range between unbounded preceding and current row) as opening_bal_qty1,
        sum(ifnull(trans_currency_opening_bal_amt, 0)) over(partition by subsidiary_id, accountnumber, transaction_type, location_id, department_id, type, market_opi_id, class_id, currency_id order by fscl_yr_nbr asc range between unbounded preceding and current row) as trans_currency_opening_bal_amt1,
        sum(ifnull(opco_currency_opening_bal_amt, 0)) over(partition by subsidiary_id, accountnumber, transaction_type, location_id, department_id, type, market_opi_id, class_id, currency_id order by fscl_yr_nbr asc range between unbounded preceding and current row) as opco_currency_opening_bal_amt1
    from ns_opco_fscl_yr_gl_opening_bal
  FULL OUTER JOIN ALL_LEDGER_COMBINATIONS
    USING (src_sys_nm, subsidiary_id, accountnumber, transaction_type, location_id, department_id, type, market_opi_id, class_id, currency_id, fscl_yr_nbr)
),
plus_one_logic as(
    select distinct 
    {{ dbt_utils.surrogate_key(['ofgo.src_sys_nm', 'ofgo.subsidiary_id', 'ofgo.accountnumber', 'ofgo.transaction_type', 'ofgo.location_id', 'ofgo.department_id', 'ofgo.type', 'ofgo.market_opi_id', 'ofgo.class_id', 'ofgo.currency_id', 'ofgo.fscl_yr_nbr']) }} as opco_fscl_yr_gl_opening_bal_sk,
    concat_ws('|', ofgo.src_sys_nm, ifnull(ofgo.subsidiary_id::string , ''), ifnull(ofgo.accountnumber::string , ''), ifnull(ofgo.transaction_type::string , ''), ifnull(ofgo.location_id::string , ''), ifnull(ofgo.department_id::string , ''), ifnull(ofgo.type::string , ''), ifnull(ofgo.market_opi_id::string , ''), ifnull(ofgo.class_id::string , ''), ifnull(ofgo.currency_id::string , ''), ifnull(ofgo.fscl_yr_nbr::string , '')) as opco_fscl_yr_gl_opening_bal_ak,
    ofgo.src_sys_nm,
    ofgo.src_key_txt,
    opco.opco_sk,
    oca.opco_chart_of_accts_sk,
    ogt.opco_gl_trans_type_sk,
    null::varchar(32) as opco_gl_trans_posting_type_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    oip.opco_purpose_sk,
    null::varchar(32) as opco_lob_sk,
    oc.opco_currency_sk as trans_currency_sk,
    fc.fscl_yr_strt_dt::date as fscl_yr_strt_dt,
    fc.fscl_yr_id::int as fscl_yr_nbr,
    case 
        when ofgo.credit_ind = 'F' then 1
        when ofgo.credit_ind = 'T' then 0
    end::numeric(1,0) as credit_ind,
    ifnull(ofgo.opening_bal_qty1, 0)::numeric(38,15) as opening_bal_qty,
    ifnull(ofgo.trans_currency_opening_bal_amt1, 0)::numeric(38,15) as trans_currency_opening_bal_amt,
    ifnull(ofgo.opco_currency_opening_bal_amt1, 0)::numeric(38,15) as opco_currency_opening_bal_amt,
    ou.opco_uom_sk,
    ob.opco_brand_sk
    from rolled_up_balance ofgo
    left join {{ ref('fscl_clndr')}} fc 
        on ofgo.fscl_yr_nbr + 1 = fc.fscl_yr_id 
    left join {{ source('NS_DEV', 'ACCOUNTS')}} a
        on ofgo.accountnumber = a.accountnumber
        and a._fivetran_deleted = false
    left join {{ ref('ns_opco_curr')}} opco 
        on ofgo.subsidiary_id = opco.opco_id
        and opco.src_sys_nm = 'NS'
    left join {{ ref('ns_opco_chart_of_accts_curr')}} oca 
        on ofgo.accountnumber = oca.gl_acct_nbr
        and oca.src_sys_nm = 'NS'
    left join {{ ref('ns_opco_gl_trans_type_curr')}} ogt 
        on ofgo.transaction_type = ogt.src_gl_trans_type_cd
        and ogt.src_sys_nm = 'NS'
    left join {{ ref('ns_opco_cost_center_curr')}} occ 
        on upper(ofgo.location_id) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'NS'
    left join {{ ref('ns_opco_dept_curr')}} od 
        on upper(ofgo.department_id) = od.src_dept_cd
        and od.src_sys_nm = 'NS'
    left join {{ ref('ns_opco_type_curr')}} ot 
        on upper(ofgo.type) = ot.src_type_cd
        and ot.src_sys_nm = 'NS'
    left join {{ ref('ns_opco_purpose_curr')}} oip
        on upper(ofgo.market_opi_id) = oip.src_purpose_cd
        and oip.src_sys_nm = 'NS'
    left join {{ ref('ns_opco_currency_curr')}} oc
        on upper(ofgo.currency_id) = oc.src_currency_cd
        and oc.src_sys_nm = 'NS'
    left join {{ ref('ns_opco_uom_curr')}} ou 
        on a.opi_qty_uom_id::varchar(15) = ou.src_uom_cd
        and ou.src_sys_nm = 'NS'
    left join {{ ref('ns_opco_brand_curr')}} ob 
        on ofgo.class_id = ob.src_brand_cd
        and ob.src_sys_nm = 'NS'
),
pre_final as (
    select  distinct
        pol.opco_fscl_yr_gl_opening_bal_sk,
        pol.opco_fscl_yr_gl_opening_bal_ak,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        pol.src_sys_nm,
        pol.src_key_txt,
        pol.opco_sk,
        pol.opco_chart_of_accts_sk,
        pol.opco_gl_trans_type_sk,
        pol.opco_gl_trans_posting_type_sk,
        pol.opco_cost_center_sk,
        pol.opco_dept_sk,
        pol.opco_type_sk,
        pol.opco_purpose_sk,
        pol.opco_lob_sk,
        pol.trans_currency_sk,
        pol.fscl_yr_strt_dt,
        pol.fscl_yr_nbr,
        pol.credit_ind,
        pol.opening_bal_qty,
        pol.trans_currency_opening_bal_amt,
        pol.opco_currency_opening_bal_amt,
        pol.opco_uom_sk,
        pol.opco_brand_sk
    from plus_one_logic pol
),
final as(
    select 
        opco_fscl_yr_gl_opening_bal_sk,
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_key_txt', 'opco_sk', 'opco_chart_of_accts_sk', 'opco_gl_trans_type_sk', 'opco_gl_trans_posting_type_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'trans_currency_sk', 'fscl_yr_strt_dt', 'fscl_yr_nbr', 'credit_ind', 'opening_bal_qty', 'trans_currency_opening_bal_amt', 'opco_currency_opening_bal_amt', 'opco_uom_sk', 'opco_brand_sk']) }} as opco_fscl_yr_gl_opening_bal_ck,
        opco_fscl_yr_gl_opening_bal_ak, 
        crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_key_txt, opco_sk, opco_chart_of_accts_sk, opco_gl_trans_type_sk, opco_gl_trans_posting_type_sk, opco_cost_center_sk, opco_dept_sk,
        opco_type_sk, opco_purpose_sk, opco_lob_sk, trans_currency_sk, fscl_yr_strt_dt, fscl_yr_nbr, credit_ind, opening_bal_qty, trans_currency_opening_bal_amt,
        opco_currency_opening_bal_amt, opco_uom_sk, opco_brand_sk
    from pre_final
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'NS_OPCO_FSCL_YR_GL_OPENING_BAL') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_FSCL_YR_GL_OPENING_BAL') and _load != 3%}
    union
    select
    opco_fscl_yr_gl_opening_bal_sk, opco_fscl_yr_gl_opening_bal_ck, opco_fscl_yr_gl_opening_bal_ak, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, src_key_txt, opco_sk, opco_chart_of_accts_sk, opco_gl_trans_type_sk, opco_gl_trans_posting_type_sk, opco_cost_center_sk, opco_dept_sk,
    opco_type_sk, opco_purpose_sk, opco_lob_sk, trans_currency_sk, fscl_yr_strt_dt, fscl_yr_nbr, credit_ind, opening_bal_qty, trans_currency_opening_bal_amt,
    opco_currency_opening_bal_amt, opco_uom_sk, opco_brand_sk
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL
    {% if _load == 1 %}
        where opco_fscl_yr_gl_opening_bal_ck not in (select distinct opco_fscl_yr_gl_opening_bal_ck from final)
    {% else %}
        where opco_fscl_yr_gl_opening_bal_ck not in (select distinct opco_fscl_yr_gl_opening_bal_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL where src_sys_nm = 'NS')
    {% endif %} 
    and src_sys_nm = 'NS'
{% endif %}