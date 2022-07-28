{% set _load = load_type('NS_OPCO_CURRENCY') %}

with ns_opco_currency as(
    select 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    currency_id::varchar(10) as src_currency_cd,
    name::varchar(50) as src_currency_nm
    from {{source('NS_DEV', 'CURRENCIES')}}
),
final as(
    select  {{dbt_utils.surrogate_key(['src_sys_nm', 'src_currency_cd'])}} as opco_currency_sk, 
            {{dbt_utils.surrogate_key(['src_sys_nm', 'src_currency_cd', 'src_currency_nm'])}} as opco_currency_ck,
            *
    from ns_opco_currency
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'NS_OPCO_CURRENCY') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CURRENCY') and _load != 3%}
    union
    select
    OPCO_CURRENCY_SK, OPCO_CURRENCY_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    SRC_SYS_NM, SRC_CURRENCY_CD, SRC_CURRENCY_NM  
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_currency
    {% if _load == 1 %}
        where opco_currency_ck not in (select distinct opco_currency_ck from final)
    {% else %}
        where opco_currency_ck not in (select distinct opco_currency_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_currency where src_sys_nm = 'NS')
    {% endif %} 
    and src_sys_nm = 'NS'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_currency_ck not in (select distinct opco_currency_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_currency where src_sys_nm = 'NS')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_currency_ck in (select distinct opco_currency_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_currency where src_sys_nm = 'NS') and delete_dtm is not null)
    {% endif %}
{% endif %}