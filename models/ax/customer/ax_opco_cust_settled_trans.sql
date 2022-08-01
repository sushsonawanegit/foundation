{% set _load = load_type('AX_OPCO_CUST_SETTLED_TRANS') %}

with ax_opco_cust_settled_trans as (
    select
    current_timestamp as crt_dtm,
    cs._fivetran_synced as stg_load_dtm,
    case
        when cs._fivetran_deleted = true then cs._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    cs.dataareaid as opco_id,
    cs.recid as src_key_txt,
    opco.opco_sk,
    oct.opco_cust_trans_sk,
    oc.opco_cust_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    oca.opco_chart_of_accts_sk as cash_dscnt_gl_acct_sk,
    cs.transdate as trans_dt,
    cs.duedate as due_dt,
    cs.createddatetime as src_crt_dtm,
    oc1.opco_cust_sk as offset_cust_sk,
    oct1.opco_cust_trans_sk as offset_cust_trans_sk,
    cs.offsettransvoucher as offset_voucher_nbr,
    cs.custcashdiscdate as cust_cash_dscnt_dt,
    cs.lastinterestdate as latest_interest_calc_dt,
    cs.canbereversed as reversible_ind,
    cs.settlementgroup as settlement_grp_txt,
    cs.settlementvoucher as settled_voucher_nbr,
    cs.settleamountcur as trans_currency_settled_amt,
    cs.settleamountmst as opco_currency_settled_amt,
    cs.exchadjustment as exch_adjstmnt_amt,
    cs.utilizedcashdisc as utilized_cash_dscnt_amt
    from {{ source('AX_DEV', 'CUSTSETTLEMENT') }} cs
    left join {{ ref('ax_opco_curr')}} opco 
        on cs.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_cust_trans_curr')}} oct
        on cs.dataareaid = oct.opco_id
        and cs.transrecid = oct.src_key_txt
        and oct.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_cust_curr')}} oc
        on cs.dataareaid = oc.opco_id
        and cs.accountnum = oc.cust_id
        and oc.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_cust_trans_curr')}} oct1
        on cs.dataareaid = oct1.opco_id
        and cs.offsetrecid = oct1.src_key_txt
        and oct1.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_cust_curr')}} oc1
        on cs.offsetcompany = oc1.opco_id
        and cs.offsetaccountnum = oc1.cust_id
        and oc1.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_cost_center_curr')}} occ 
        on upper(cs.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_dept_curr')}} od 
        on upper(cs.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_type_curr')}} ot 
        on upper(cs.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_purpose_curr')}} op 
        on upper(cs.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_lob_curr')}} ol 
        on upper(cs.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_chart_of_accts_curr')}} oca 
        on cs.dataareaid = oca.opco_id
        and cs.cashdiscaccount = oca.gl_acct_nbr
        and oca.src_sys_nm = 'AX'
    where cs.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as(
    select distinct
           {{dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_key_txt'])}} as opco_cust_settled_trans_sk,
           {{dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_key_txt', 'opco_sk', 'opco_cust_trans_sk', 'opco_cust_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'cash_dscnt_gl_acct_sk', 'trans_dt', 'due_dt', 'src_crt_dtm', 'offset_cust_sk', 'offset_cust_trans_sk', 'offset_voucher_nbr', 'cust_cash_dscnt_dt', 'latest_interest_calc_dt', 'reversible_ind', 'settlement_grp_txt', 'settled_voucher_nbr', 'trans_currency_settled_amt', 'opco_currency_settled_amt', 'exch_adjstmnt_amt', 'utilized_cash_dscnt_amt'])}} as opco_cust_settled_trans_ck,
           concat_ws('|', src_sys_nm, opco_id, src_key_txt) as opco_cust_settled_trans_ak, *
    from ax_opco_cust_settled_trans
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_CUST_SETTLED_TRANS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CUST_SETTLED_TRANS') and _load != 3%}
    union
    select
    opco_cust_settled_trans_sk, opco_cust_settled_trans_ck, opco_cust_settled_trans_ak, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, opco_id, src_key_txt, opco_sk, opco_cust_trans_sk, opco_cust_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk,
    cash_dscnt_gl_acct_sk, trans_dt, due_dt, src_crt_dtm, offset_cust_sk, offset_cust_trans_sk, offset_voucher_nbr, cust_cash_dscnt_dt, latest_interest_calc_dt, 
    reversible_ind, settlement_grp_txt, settled_voucher_nbr, trans_currency_settled_amt, opco_currency_settled_amt, exch_adjstmnt_amt, utilized_cash_dscnt_amt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_settled_trans
    {% if _load == 1 %}
        where opco_cust_settled_trans_ck not in (select distinct opco_cust_settled_trans_ck from final)
    {% else %}
        where opco_cust_settled_trans_ck not in (select distinct opco_cust_settled_trans_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_settled_trans where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_settled_trans_ck not in (select distinct opco_cust_settled_trans_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_settled_trans where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_settled_trans_ck in (select distinct opco_cust_settled_trans_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_settled_trans where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}