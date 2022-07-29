{% set _load = load_type('AX_OPCO_CUST_OPEN_TRANS') %}

with ax_opco_cust_open_trans as (
    select 
    current_timestamp as crt_dtm,
    cto._fivetran_synced as stg_load_dtm,
    case 
        when cto._fivetran_deleted = true then cto._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    cto.dataareaid as opco_id,
    cto.recid as src_key_txt,
    oct.opco_cust_trans_sk,
    opco.opco_sk,
    oc.opco_cust_sk,
    oca.opco_chart_of_accts_sk as cash_dscnt_gl_acct_sk,
    cto.transdate as trans_dt,
    cto.duedate as due_dt,
    cto.lastinterestdate as ltst_interest_calc_dt,
    cto.cashdiscdate as cash_dscnt_dt,
    case
        when cto.usecashdisc = 0 then 'Normal'
        when cto.usecashdisc = 1 then 'Always'
        when cto.usecashdisc = 2 then 'Never'
    end as cash_dscnt_usage_txt,
    cto.amountcur as trans_currency_trans_amt,
    cto.amountmst as opco_currency_trans_amt,
    cto.possiblecashdisc as included_cash_dscnt_amt
    from {{ source('AX_DEV', 'CUSTTRANSOPEN') }} cto 
    left join {{ ref('v_ax_opco_cust_trans_curr')}} oct 
        on cto.dataareaid = oct.opco_id
        and cto.refrecid = oct.src_key_txt
        and oct.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_curr')}} opco 
        on cto.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_cust_curr')}} oc 
        on cto.dataareaid = oc.opco_id
        and cto.accountnum = oc.cust_id
        and oc.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_chart_of_accts_curr')}} oca 
        on cto.dataareaid = oca.opco_id
        and cto.cashdiscaccount = oca.gl_acct_nbr
        and oca.src_sys_nm = 'AX'
    where cto.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select {{dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_key_txt'])}} as opco_cust_open_trans_sk,
           {{dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_key_txt', 'opco_cust_trans_sk', 'opco_sk', 'opco_cust_sk', 'cash_dscnt_gl_acct_sk', 'trans_dt', 'due_dt', 'ltst_interest_calc_dt', 'cash_dscnt_dt', 'cash_dscnt_usage_txt', 'trans_currency_trans_amt', 'opco_currency_trans_amt', 'included_cash_dscnt_amt'])}} as opco_cust_open_trans_ck,
           concat_ws('|', src_sys_nm, opco_id, src_key_txt) as opco_cust_open_trans_ak,
           *
    from ax_opco_cust_open_trans 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_CUST_OPEN_TRANS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'AX_OPCO_CUST_OPEN_TRANS') and _load != 3%}
    union
    select
    opco_cust_open_trans_sk, opco_cust_open_trans_CK, opco_cust_open_trans_ak, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, opco_id, src_key_txt, opco_cust_trans_sk, opco_sk, opco_cust_sk, cash_dscnt_gl_acct_sk, trans_dt, due_dt, ltst_interest_calc_dt, cash_dscnt_dt, cash_dscnt_usage_txt, trans_currency_trans_amt, opco_currency_trans_amt, included_cash_dscnt_amt 
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_open_trans
    {% if _load == 1 %}
        where opco_cust_open_trans_CK not in (select distinct opco_cust_open_trans_CK from final)
    {% else %}
        where opco_cust_open_trans_CK not in (select distinct opco_cust_open_trans_CK from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_open_trans where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_open_trans_CK not in (select distinct opco_cust_open_trans_CK from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_open_trans where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_open_trans_CK in (select distinct opco_cust_open_trans_CK from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_open_trans where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}