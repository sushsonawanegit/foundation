{% set _load = load_type('EPCR_EU_OPCO_CURRENCY') %}

with epcr_eu_opco_currency as(
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'EPCR-EU' as src_sys_nm,
    upper(trim(currencycode)) as src_currency_cd,
    max(currname) as src_currency_nm
    from {{source('EPCR_EU_DEV', 'CURRENCY')}}
    where currencycode not in ('CUBAUS01','CUBAUS02') 
    group by src_sys_nm, src_currency_cd, _fivetran_deleted

    ),
final as(
    select 
        {{dbt_utils.surrogate_key(['src_sys_nm', 'src_currency_cd'])}} as opco_currency_sk,
        {{dbt_utils.surrogate_key(['src_sys_nm', 'src_currency_cd', 'src_currency_nm'])}} as opco_currency_ck, 
        *
    from epcr_eu_opco_currency

)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'EPCR_EU_OPCO_CURRENCY') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CURRENCY') and _load != 3%}
    union
    select
    opco_currency_sk, opco_currency_ck, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, src_currency_cd, src_currency_nm  
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_currency
    {% if _load == 1 %}
        where opco_currency_ck not in (select distinct opco_currency_ck from final)
    {% else %}
        where opco_currency_ck not in (select distinct opco_currency_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_currency where src_sys_nm = 'EPCR-EU')
    {% endif %} 
    and src_sys_nm = 'EPCR-EU'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_currency_ck not in (select distinct opco_currency_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_currency where src_sys_nm = 'EPCR-EU')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_currency_ck in (select distinct opco_currency_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_currency where src_sys_nm = 'EPCR-EU') and delete_dtm is not null)
    {% endif %}
{% endif %}