{% set _load = load_type('AX_OPCO_FSCL_YR_GL_OPENING_BAL') %}

with opco_fscl_yr_gl_opening_bal as(
    select 
    current_timestamp as crt_dtm,
    lt._fivetran_synced as stg_load_dtm,
    case 
        when lt._fivetran_deleted = true then lt._fivetran_synced
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    lt.recid::varchar(50) as src_key_txt,
    opco.opco_sk,
    oca.opco_chart_of_accts_sk,
    ogt.opco_gl_trans_type_sk,
    ogtp.opco_gl_trans_posting_type_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    oip.opco_purpose_sk,
    ol.opco_lob_sk,
    oc.opco_currency_sk as trans_currency_sk,
    lt.transdate::date as fscl_yr_strt_dt,
    case    
        when month(lt.transdate) = 12 then year(lt.transdate)+1
        else year(lt.transdate)
    end::int as fscl_yr_nbr,
    lt.crediting::numeric(1,0) as credit_ind,
    lt.qty::float as opening_bal_qty,
    lt.amountcur::float as trans_currency_opening_bal_amt,
    lt.amountmst::float as opco_currency_opening_bal_amt,
    ou.opco_uom_sk,
    null::varchar(32) as opco_brand_sk
    from {{ source('AX_DEV', 'LEDGERTRANS') }} lt
    left join {{ source('AX_DEV', 'LEDGERTABLE')}} ltb 
        on lt.accountnum = ltb.accountnum
        and lt.dataareaid = ltb.dataareaid
        and ltb.dataareaid not in {{ var('excluded_ax_companies')}}
        and ltb._fivetran_deleted = false
    left join {{ ref('opco')}} opco 
        on lt.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ ref('opco_chart_of_accts')}} oca 
        on lt.dataareaid = oca.opco_id
        and lt.accountnum = oca.gl_acct_nbr
        and oca.src_sys_nm = 'AX'
    left join {{ ref('opco_gl_trans_type')}} ogt 
        on lt.transtype::string = ogt.src_gl_trans_type_cd
        and ogt.src_sys_nm = 'AX'
    left join {{ ref('opco_gl_trans_posting_type')}} ogtp 
        on lt.posting = ogtp.src_gl_trans_posting_type_cd
        and ogtp.src_sys_nm = 'AX'
    left join {{ ref('opco_cost_center')}} occ 
        on upper(lt.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ ref('opco_dept')}} od 
        on upper(lt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ ref('opco_type')}} ot 
        on upper(lt.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ ref('opco_purpose')}} oip
        on upper(lt.dimension4_) = oip.src_purpose_cd
        and oip.src_sys_nm = 'AX'
    left join {{ ref('opco_lob')}} ol
        on upper(lt.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('opco_currency')}} oc
        on upper(lt.currencycode) = oc.src_currency_cd
        and oc.src_sys_nm = 'AX'
    left join {{ ref('opco_uom')}} ou 
        on upper(ltb.unitid_opi) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    where lt.periodcode = 0
    and lt.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select  distinct
            {{ dbt_utils.surrogate_key(['ogo.src_sys_nm', 'ogo.src_key_txt']) }} as opco_fscl_yr_gl_opening_bal_sk, 
            {{ dbt_utils.surrogate_key(['ogo.src_sys_nm', 'ogo.src_key_txt', 'ogo.opco_sk', 'ogo.opco_chart_of_accts_sk', 'ogo.opco_gl_trans_type_sk', 'ogo.opco_gl_trans_posting_type_sk', 'ogo.opco_cost_center_sk', 'ogo.opco_dept_sk', 'ogo.opco_type_sk', 'ogo.opco_purpose_sk', 'ogo.opco_lob_sk', 'ogo.trans_currency_sk', 'ogo.fscl_yr_strt_dt', 'ogo.fscl_yr_nbr', 'ogo.opening_bal_qty', 'ogo.trans_currency_opening_bal_amt', 'ogo.opco_currency_opening_bal_amt']) }} as opco_fscl_yr_gl_opening_bal_ck,
            concat_ws('|', ogo.src_sys_nm, ogo.src_key_txt)::varchar(100) as opco_fscl_yr_gl_opening_bal_ak,
            * 
    from opco_fscl_yr_gl_opening_bal ogo
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_FSCL_YR_GL_OPENING_BAL') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_FSCL_YR_GL_OPENING_BAL') and _load != 3%}
    union
    select
    opco_fscl_yr_gl_opening_bal_sk, opco_fscl_yr_gl_opening_bal_ck, opco_fscl_yr_gl_opening_bal_ak, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, src_key_txt, opco_sk, opco_chart_of_accts_sk, opco_gl_trans_type_sk, opco_gl_trans_posting_type_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk,
    opco_lob_sk, trans_currency_sk, fscl_yr_strt_dt, fscl_yr_nbr, opening_bal_qty, trans_currency_opening_bal_amt, opco_currency_opening_bal_amt, opco_uom_sk, opco_brand_sk
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL
    {% if _load == 1 %}
        where opco_fscl_yr_gl_opening_bal_ck not in (select distinct opco_fscl_yr_gl_opening_bal_ck from final)
    {% else %}
        where opco_fscl_yr_gl_opening_bal_ck not in (select distinct opco_fscl_yr_gl_opening_bal_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_fscl_yr_gl_opening_bal_ck not in (select distinct opco_fscl_yr_gl_opening_bal_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_fscl_yr_gl_opening_bal_ck in (select distinct opco_fscl_yr_gl_opening_bal_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_FSCL_YR_GL_OPENING_BAL where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}