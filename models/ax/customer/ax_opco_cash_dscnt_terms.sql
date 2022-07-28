{% set _load = load_type('AX_OPCO_CASH_DSCNT_TERMS') %}

with ax_opco_cash_dscnt_terms as(
    select
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    upper(cashdisccode)::varchar(20) as src_cash_dscnt_terms_cd,
    max(description)::varchar(100) as src_cash_dscnt_terms_desc,
    max(numofdays)::number(4) as pymnt_period_in_days_nbr,
    max(percent_)::float as dscnt_pct
    from {{source('AX_DEV', 'CASHDISC')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_cash_dscnt_terms_cd, _fivetran_deleted
),
final as(
    select 
        {{dbt_utils.surrogate_key(['src_sys_nm', 'src_cash_dscnt_terms_cd'])}} as opco_CASH_DSCNT_TERMS_sk,
        {{dbt_utils.surrogate_key(['src_sys_nm', 'src_cash_dscnt_terms_cd', 'src_cash_dscnt_terms_desc','pymnt_period_in_days_nbr','dscnt_pct'])}} as opco_CASH_DSCNT_TERMS_ck, 
        *
    from ax_opco_CASH_DSCNT_TERMS
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_CASH_DSCNT_TERMS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CASH_DSCNT_TERMS') and _load != 3%}
    union
    select
    opco_CASH_DSCNT_TERMS_sk, opco_CASH_DSCNT_TERMS_ck, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, src_cash_dscnt_terms_cd, src_cash_dscnt_terms_desc, pymnt_period_in_days_nbr, dscnt_pct
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_CASH_DSCNT_TERMS
    {% if _load == 1 %}
        where opco_CASH_DSCNT_TERMS_ck not in (select distinct opco_CASH_DSCNT_TERMS_ck from final)
    {% else %}
        where opco_CASH_DSCNT_TERMS_ck not in (select distinct opco_CASH_DSCNT_TERMS_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_CASH_DSCNT_TERMS where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}

{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_CASH_DSCNT_TERMS_ck not in (select distinct opco_CASH_DSCNT_TERMS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_CASH_DSCNT_TERMS where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_CASH_DSCNT_TERMS_ck in (select distinct opco_CASH_DSCNT_TERMS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_CASH_DSCNT_TERMS where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}