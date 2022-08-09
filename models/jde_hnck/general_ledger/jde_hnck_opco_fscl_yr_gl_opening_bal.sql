{% set _load = load_type('JDE_HNCK_OPCO_FSCL_YR_GL_OPENING_BAL') %}

with jde_hnck_opco_fscl_yr_gl_opening_bal as(
    select 
    {{ dbt_utils.surrogate_key(["'JDE-HNCK'", 'f0902.gbco', 'f0902.gbmcu', 'f0902.gbobj', 'f0902.gbsub', 'f0902.gbsblt', 'f0902.gbsbl', 'f0902.gbctry', 'f0902.gbfy'])}} as opco_fscl_yr_gl_opening_bal_sk,
    concat_ws('|', 'JDE-HNCK', trim(f0902.gbco), trim(f0902.gbmcu), trim(f0902.gbobj), trim(f0902.gbsub), trim(f0902.gbsblt), trim(f0902.gbsbl), trim(f0902.gbctry), trim(f0902.gbfy)) as opco_fscl_yr_gl_opening_bal_ak,
    current_timestamp as crt_dtm,
    F0902.mule_load_ts as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'JDE-HNCK' as src_sys_nm,
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
    iff(trim(f0902.gbapyc) < 0, 1, 0) as credit_ind,
    iff(trim(f0902.gblt) = 'AU', trim(f0902.gbapyc)/(select pow(10, trim(frcdec)::number) from {{ source('JDE_HNCK_DEV3', 'F9210') }} where trim(frdtai) = 'APYC'), null) as opening_bal_qty,
    iff(trim(f0902.gblt) = 'AA', trim(f0902.gbapyc)/(select pow(10, trim(frcdec)::number) from {{ source('JDE_HNCK_DEV3', 'F9210') }} where trim(frdtai) = 'APYC'), null) as trans_currency_opening_bal_amt,
    iff(trim(f0902.gblt) = 'AA', trim(f0902.gbapyc)/(select pow(10, trim(frcdec)::number) from {{ source('JDE_HNCK_DEV3', 'F9210') }} where trim(frdtai) = 'APYC'), null) as opco_currency_opening_bal_amt,
    null as opco_uom_sk,
    null as opco_brand_sk,
    oslt.opco_sub_ledger_type_sk,
    trim(f0902.gbsbl) as sub_ledger_cd
    from {{ source('JDE_HNCK_DEV1', 'F0902') }} f0902
    left join {{ ref('jde_hnck_opco_curr')}} opco 
        on upper(trim(f0902.gbco)) = opco.opco_id
        and opco.src_sys_nm = 'JDE-HNCK'
    left join {{ ref('jde_hnck_opco_chart_of_accts_curr')}} ocoa 
        on iff(trim(f0902.gbsub) <> '', concat_ws('.', upper(trim(f0902.gbmcu)), trim(f0902.gbobj), trim(f0902.gbsub)), concat_ws('.', upper(trim(f0902.gbmcu)), trim(f0902.gbobj))) = ocoa.gl_acct_nbr
        and ocoa.src_sys_nm = 'JDE-HNCK'
    left join {{ref('jde_hnck_opco_cost_center_curr')}} occ 
        on upper(trim(f0902.gbmcu)) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'JDE-HNCK'
    left join {{ref('jde_hnck_opco_currency_curr')}} oc 
        on upper(trim(f0902.gbcrcd)) = oc.src_currency_cd
        and oc.src_sys_nm = 'JDE-HNCK'
    left join {{ref('fscl_clndr')}} fc 
        on concat(trim(f0902.gbctry), trim(f0902.gbfy)) = fc.fscl_yr_id   
    left join {{ref('jde_hnck_opco_sub_ledger_type_curr')}} oslt 
        on upper(trim(f0902.gbsblt)) = oslt.src_sub_ledger_type_cd
        and oslt.src_sys_nm = 'JDE-HNCK'
    where trim(f0902.gbco) = '03000' and trim(f0902.gblt) in ('AA', 'AU')
    and iff(trim(f0902.gbsub) <> '', concat_ws('.', upper(trim(f0902.gbmcu)), trim(f0902.gbobj), trim(f0902.gbsub)), concat_ws('.', upper(trim(f0902.gbmcu)), trim(f0902.gbobj))) not in ('30.6001', '30.6002', '30.6003', '30.6004', '30.6005', '30.6006', '30.6020', '30.6021', '30.6022', '30.6023', '30.6026', '30.6028', '30.6030', '30.6032', '30.6034', '31.6001', '31.6002', '31.6004', '31.6005', '31.6006', '31.6020', '31.6021', '31.6022', '31.6023', '31.6026', '31.6028', '31.6030', '31.6032', '31.6034', '32.6001', '32.6002', '32.6004', '32.6005', '32.6020', '32.6021', '32.6022', '32.6023', '32.6026', '32.6028', '32.6030', '32.6032', '32.6034', '33.6001', '33.6002', '33.6004', '33.6004.1', '33.6005', '33.6006', '33.6020', '33.6021', '33.6022', '33.6023', '33.6026', '33.6028', '33.6030', '33.6032', '33.6034', '301.2102', '301.5201', '301.5210', '301.5210.3', '301.5290', '301.6001', '301.6002', '301.6004', '301.6004.1', '301.6005', '301.6006', '301.6007', '301.6020', '301.6021', '301.6022', '301.6023', '301.6026', '301.6028', '301.6030', '301.6032', '301.6034', '302.2102', '302.5201', '302.5210', '302.5210.3', '302.5290', '302.6001', '302.6002', '302.6004', '302.6004.1', '302.6005', '302.6006', '302.6007', '302.6020', '302.6021', '302.6022', '302.6023', '302.6026', '302.6028', '302.6030', '302.6032', '302.6034', '303.2102', '303.5201', '303.5210', '303.5210.3', '303.5290', '303.6001', '303.6002', '303.6004', '303.6004.1', '303.6005', '303.6006', '303.6007', '303.6020', '303.6021', '303.6022', '303.6023', '303.6026', '303.6028', '303.6030', '303.6032', '303.6034', '304.2102', '304.5201', '304.5210', '304.5210.3', '304.5290', '304.6001', '304.6002', '304.6004', '304.6004.1', '304.6005', '304.6006', '304.6007', '304.6020', '304.6021', '304.6022', '304.6023', '304.6026', '304.6028', '304.6030', '304.6032', '304.6034', '306.2102', '306.5201', '306.5210', '306.5210.3', '306.5290', '306.6001', '306.6002', '306.6004', '306.6004.1', '306.6005', '306.6006', '306.6007', '306.6020', '306.6021', '306.6022', '306.6023', '306.6026', '306.6028', '306.6030', '306.6032', '306.6034', '307.2102', '307.5201', '307.5210', '307.5210.3', '307.5290', '307.6001', '307.6002', '307.6004', '307.6004.1', '307.6005', '307.6006', '307.6007', '307.6020', '307.6021', '307.6022', '307.6023', '307.6026', '307.6028', '307.6030', '307.6032', '307.6034', '312.6001', '312.6002', '312.6004', '312.6004.1', '312.6005', '312.6006', '312.6020', '312.6021', '312.6022', '312.6023', '312.6026', '312.6028', '312.6030', '312.6032', '312.6034', '322.6001', '322.6002', '322.6004', '322.6005', '322.6006', '322.6020', '322.6021', '322.6022', '322.6023', '322.6026', '322.6028', '322.6030', '322.6032', '322.6034', '332.6001', '332.6002', '332.6004', '332.6005', '332.6006', '332.6020', '332.6021', '332.6022', '332.6023', '332.6026', '332.6028', '332.6030', '332.6032', '332.6034', '342.6001', '342.6002', '342.6004', '342.6005', '342.6006', '342.6020', '342.6021', '342.6022', '342.6023', '342.6026', '342.6028', '342.6030', '342.6032', '342.6034', '362.6001', '362.6002', '362.6004', '362.6005', '362.6006', '362.6020', '362.6021', '362.6022', '362.6023', '362.6026', '362.6028', '362.6030', '362.6032', '362.6034', '372.6001', '372.6002', '372.6004', '372.6005', '372.6006', '372.6020', '372.6021', '372.6022', '372.6023', '372.6026', '372.6028', '372.6030', '372.6032', '372.6034', '3000.2102', '3000.2103', '3000.6001', '3000.6002', '3000.6004', '3000.6004.1', '3000.6005', '3000.6006', '3000.6007', '3000.6020', '3000.6021', '3000.6022', '3000.6023', '3000.6026', '3000.6028', '3000.6030', '3000.6032', '3000.6034', '3000.6161')
),
pre_final as(
    select distinct 
    opco_fscl_yr_gl_opening_bal_sk, opco_fscl_yr_gl_opening_bal_ak,
    crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_key_txt, opco_sk, opco_chart_of_accts_sk, opco_gl_trans_type_sk, opco_gl_trans_posting_type_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, trans_currency_sk, fscl_yr_strt_dt, fscl_yr_nbr, credit_ind, opening_bal_qty, trans_currency_opening_bal_amt, opco_currency_opening_bal_amt, opco_uom_sk, opco_brand_sk, opco_sub_ledger_type_sk, sub_ledger_cd 
    from jde_hnck_opco_fscl_yr_gl_opening_bal
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
    {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_key_txt', 'opco_sk', 'opco_chart_of_accts_sk', 'opco_gl_trans_type_sk', 'opco_gl_trans_posting_type_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'trans_currency_sk', 'fscl_yr_strt_dt', 'fscl_yr_nbr', 'credit_ind', 'opening_bal_qty', 'trans_currency_opening_bal_amt', 'opco_currency_opening_bal_amt', 'opco_uom_sk', 'opco_brand_sk', 'opco_sub_ledger_type_sk', 'sub_ledger_cd']) }} as opco_fscl_yr_gl_opening_bal_ck,
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
    coalesce(ac.sub_ledger_cd , qc.sub_ledger_cd) as sub_ledger_cd
    from amt_cte ac 
    full outer join qty_cte qc 
        using(opco_fscl_yr_gl_opening_bal_sk)
),
delete_captured as (
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'JDE_HNCK_OPCO_FSCL_YR_GL_OPENING_BAL') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_FSCL_YR_GL_OPENING_BAL') and _load != 3%}
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
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL where src_sys_nm = 'JDE-HNCK')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_FSCL_YR_GL_OPENING_BAL_ck not in (select distinct OPCO_FSCL_YR_GL_OPENING_BAL_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL where src_sys_nm = 'JDE-HNCK')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_FSCL_YR_GL_OPENING_BAL_ck in (select distinct OPCO_FSCL_YR_GL_OPENING_BAL_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL where src_sys_nm = 'JDE-HNCK') and delete_dtm is not null)
    {% endif %}
{% endif %}