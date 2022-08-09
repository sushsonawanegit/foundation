{% set _load = load_type('NPP_OPCO_FSCL_YR_GL_OPENING_BAL') %}

with  opco_fscl_yr_gl_opening_bal as(
    select 
    {{ dbt_utils.surrogate_key(["'JDE-NPP'", 'trim(f0902.gbco)', 'trim(f0902.gbmcu)', 'trim(f0902.gbobj)', 'trim(f0902.gbsub)', 'trim(f0902.gbsblt)', 'trim(f0902.gbsbl)', 'trim(f0902.gbctry)', 'trim(f0902.gbfy)'])}} as opco_fscl_yr_gl_opening_bal_sk,
    concat_ws('|', 'JDE-NPP', trim(f0902.gbco), trim(f0902.gbmcu), trim(f0902.gbobj), trim(f0902.gbsub), trim(f0902.gbsblt), trim(f0902.gbsbl), trim(f0902.gbctry), trim(f0902.gbfy)) as opco_fscl_yr_gl_opening_bal_ak,
    current_timestamp as crt_dtm,
    f0902.mule_load_ts as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'JDE-NPP' as src_sys_nm,
    null as src_key_txt,
    opco.opco_sk,
    ocoa.opco_chart_of_accts_sk,
    null as opco_gl_trans_type_sk,
    null as opco_gl_trans_posting_type_sk,
    occ.opco_cost_center_sk,
    null as opco_dept_sk,
    null as opco_type_sk,
    null as opco_purpose_sk,
    null as opco_lob_sk,
    oc.opco_currency_sk as trans_currency_sk,
    fc.fscl_yr_strt_dt,
    concat(trim(f0902.gbctry), trim(f0902.gbfy)) as fscl_yr_nbr,
    iff(trim(f0902.gbapyc)::float < 0, 1, 0) as credit_ind,
    iff(trim(f0902.gblt) = 'AU', trim(f0902.gbapyc)::float/100, null) as opening_bal_qty,
    iff(trim(f0902.gblt) = 'AA', trim(f0902.gbapyc)::float/100, null) as trans_currency_opening_bal_amt,
    iff(trim(f0902.gblt) = 'AA', trim(f0902.gbapyc)::float/(select pow(10, trim(frcdec)::number) from {{ source('JDE_HNCK_DEV3', 'F9210') }} where trim(frdtai) = 'APYC'), null) as opco_currency_opening_bal_amt,
    null as opco_uom_sk,
    null as opco_brand_sk,
    oslt.opco_sub_ledger_type_sk,
    trim(f0902.gbsbl) as sub_ledger_cd
    from {{ source('JDE_NPP_DEV1', 'F0902') }} f0902
    left join {{ ref('jde_npp_opco_curr')}} opco 
        on upper(trim(f0902.gbco)) = opco.opco_id
        and opco.src_sys_nm = 'JDE-NPP'
    left join {{ ref('jde_npp_opco_chart_of_accts_curr')}} ocoa 
        on iff(trim(f0902.gbsub) <> '', concat_ws('.', upper(trim(f0902.gbmcu)), trim(f0902.gbobj), trim(f0902.gbsub)), concat_ws('.', upper(trim(f0902.gbmcu)), trim(f0902.gbobj))) = ocoa.gl_acct_nbr
        and ocoa.src_sys_nm = 'JDE-NPP'
    left join {{ref('jde_npp_opco_cost_center_curr')}} occ 
        on upper(trim(f0902.gbmcu)) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'JDE-NPP'
    left join {{ref('jde_npp_opco_currency_curr')}} oc 
        on upper(trim(f0902.gbcrcd)) = oc.src_currency_cd
        and oc.src_sys_nm = 'JDE-NPP'
    left join {{ref('fscl_clndr')}} fc 
        on concat(trim(f0902.gbctry), trim(f0902.gbfy)) = fc.fscl_yr_id   
    left join {{ref('jde_npp_opco_sub_ledger_type_curr')}} oslt 
        on upper(trim(f0902.gbsblt)) = oslt.src_sub_ledger_type_cd
        and oslt.src_sys_nm = 'JDE-NPP'
    where trim(f0902.gbco) in ('00001', '00006', '00009') and trim(f0902.gblt) in ('AA', 'AU')
),
pre_final as(
    select distinct 
    opco_fscl_yr_gl_opening_bal_sk, opco_fscl_yr_gl_opening_bal_ak,
    crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_key_txt, opco_sk, opco_chart_of_accts_sk, opco_gl_trans_type_sk, opco_gl_trans_posting_type_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, trans_currency_sk, fscl_yr_strt_dt, fscl_yr_nbr, credit_ind, opening_bal_qty, trans_currency_opening_bal_amt, opco_currency_opening_bal_amt, opco_uom_sk, opco_brand_sk, opco_sub_ledger_type_sk, sub_ledger_cd 
    from opco_fscl_yr_gl_opening_bal
),
qty_cte as (
    select distinct
    opco_fscl_yr_gl_opening_bal_sk, opco_fscl_yr_gl_opening_bal_ak,
    crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_key_txt, opco_sk, opco_chart_of_accts_sk, opco_gl_trans_type_sk, opco_gl_trans_posting_type_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, trans_currency_sk, fscl_yr_strt_dt, fscl_yr_nbr, credit_ind, opening_bal_qty, trans_currency_opening_bal_amt, opco_currency_opening_bal_amt, opco_uom_sk, opco_brand_sk, opco_sub_ledger_type_sk, sub_ledger_cd 
    from pre_final
    where opening_bal_qty is not null
),
amt_cte as (
    select distinct 
    opco_fscl_yr_gl_opening_bal_sk, opco_fscl_yr_gl_opening_bal_ak,
    crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_key_txt, opco_sk, opco_chart_of_accts_sk, opco_gl_trans_type_sk, opco_gl_trans_posting_type_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, trans_currency_sk, fscl_yr_strt_dt, fscl_yr_nbr, credit_ind, opening_bal_qty, trans_currency_opening_bal_amt, opco_currency_opening_bal_amt, opco_uom_sk, opco_brand_sk, opco_sub_ledger_type_sk, sub_ledger_cd 
    from pre_final
    where opening_bal_qty is null
),
final as (
    select distinct
    coalesce(ac.opco_fscl_yr_gl_opening_bal_sk, qc.opco_fscl_yr_gl_opening_bal_sk) as opco_fscl_yr_gl_opening_bal_sk,
    coalesce(ac.opco_fscl_yr_gl_opening_bal_ak, qc.opco_fscl_yr_gl_opening_bal_ak) as opco_fscl_yr_gl_opening_bal_ak,
    coalesce(ac.crt_dtm, qc.crt_dtm) as crt_dtm,
    coalesce(ac.stg_load_dtm, qc.stg_load_dtm) as stg_load_dtm,
    coalesce(ac.delete_dtm, qc.delete_dtm) as delete_dtm,
    coalesce(ac.src_sys_nm, qc.src_sys_nm) as src_sys_nm,
    coalesce(ac.src_key_txt, qc.src_key_txt) as src_key_txt,
    coalesce(ac.opco_sk, qc.opco_sk) as opco_sk,
    coalesce(ac.opco_chart_of_accts_sk, qc.opco_chart_of_accts_sk) as opco_chart_of_accts_sk,
    coalesce(ac.opco_gl_trans_type_sk, qc.opco_gl_trans_type_sk) as opco_gl_trans_type_sk,
    coalesce(ac.opco_gl_trans_posting_type_sk, qc.opco_gl_trans_posting_type_sk) as opco_gl_trans_posting_type_sk,
    coalesce(ac.opco_cost_center_sk, qc.opco_cost_center_sk) as opco_cost_center_sk,
    coalesce(ac.opco_dept_sk, qc.opco_dept_sk) as opco_dept_sk,
    coalesce(ac.opco_type_sk, qc.opco_type_sk) as opco_type_sk,
    coalesce(ac.opco_purpose_sk, qc.opco_purpose_sk) as opco_purpose_sk,
    coalesce(ac.opco_lob_sk, qc.opco_lob_sk) as opco_lob_sk,
    coalesce(ac.trans_currency_sk, qc.trans_currency_sk) as trans_currency_sk,
    coalesce(ac.fscl_yr_strt_dt, qc.fscl_yr_strt_dt) as fscl_yr_strt_dt,
    coalesce(ac.fscl_yr_nbr, qc.fscl_yr_nbr) as fscl_yr_nbr,
    case
        when ac.trans_currency_opening_bal_amt is null and ac.opco_currency_opening_bal_amt is null then qc.credit_ind
        else ac.credit_ind
    end as credit_ind,
    coalesce(qc.opening_bal_qty, ac.opening_bal_qty) as opening_bal_qty,
    coalesce(ac.trans_currency_opening_bal_amt, qc.trans_currency_opening_bal_amt) as trans_currency_opening_bal_amt,
    coalesce(ac.opco_currency_opening_bal_amt, qc.opco_currency_opening_bal_amt) as opco_currency_opening_bal_amt,
    coalesce(ac.opco_uom_sk, qc.opco_uom_sk) as opco_uom_sk,
    coalesce(ac.opco_brand_sk, qc.opco_brand_sk) as opco_brand_sk,
    coalesce(ac.opco_sub_ledger_type_sk, qc.opco_sub_ledger_type_sk) as opco_sub_ledger_type_sk,
    coalesce(ac.sub_ledger_cd , qc.sub_ledger_cd) as sub_ledger_cd,
    {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_key_txt', 'opco_sk', 'opco_chart_of_accts_sk', 'opco_gl_trans_type_sk', 'opco_gl_trans_posting_type_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'trans_currency_sk', 'fscl_yr_strt_dt', 'fscl_yr_nbr', 'credit_ind', 'opening_bal_qty', 'trans_currency_opening_bal_amt', 'opco_currency_opening_bal_amt', 'opco_uom_sk', 'opco_brand_sk', 'opco_sub_ledger_type_sk', 'sub_ledger_cd']) }} as opco_fscl_yr_gl_opening_bal_ck
    from amt_cte ac 
    full outer join qty_cte qc 
        using(opco_fscl_yr_gl_opening_bal_sk)
),
delete_captured as (
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'NPP_OPCO_FSCL_YR_GL_OPENING_BAL') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_FSCL_YR_GL_OPENING_BAL') and _load != 3%}
        union
        select
        OPCO_FSCL_YR_GL_OPENING_BAL_SK, OPCO_FSCL_YR_GL_OPENING_BAL_CK, opco_fscl_yr_gl_opening_bal_ak, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, src_key_txt, opco_sk, opco_chart_of_accts_sk, opco_gl_trans_type_sk, opco_gl_trans_posting_type_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, trans_currency_sk, fscl_yr_strt_dt, fscl_yr_nbr, credit_ind, opening_bal_qty, trans_currency_opening_bal_amt, opco_currency_opening_bal_amt, opco_uom_sk, opco_brand_sk, opco_sub_ledger_type_sk, sub_ledger_cd

        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL
        {% if _load == 1 %}
            where OPCO_FSCL_YR_GL_OPENING_BAL_ck not in (select distinct OPCO_FSCL_YR_GL_OPENING_BAL_ck from final)
        {% else %}
            where OPCO_FSCL_YR_GL_OPENING_BAL_ck not in (select distinct OPCO_FSCL_YR_GL_OPENING_BAL_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL where src_sys_nm = 'JDE-NPP')
        {% endif %} 
        and src_sys_nm = 'JDE-NPP'
    {% endif %}
)

select * from delete_captured


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_FSCL_YR_GL_OPENING_BAL_ck not in (select distinct OPCO_FSCL_YR_GL_OPENING_BAL_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL where src_sys_nm = 'JDE-NPP')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_FSCL_YR_GL_OPENING_BAL_ck in (select distinct OPCO_FSCL_YR_GL_OPENING_BAL_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL where src_sys_nm = 'JDE-NPP') and delete_dtm is not null)
    {% endif %}
{% endif %}