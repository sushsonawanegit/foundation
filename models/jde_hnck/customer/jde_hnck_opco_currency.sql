{% set _load = load_type('JDE_HNCK_OPCO_CURRENCY') %}

with jde_hnck_opco_currency as(
    select 
    current_timestamp as crt_dtm,
    mule_load_ts as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'JDE-HNCK'::varchar(20) as src_sys_nm,
    upper(trim(cvcrcd))::varchar(10) as src_currency_cd,
    trim(cvdl01)::varchar(50) as src_currency_nm
    from {{source('JDE_HNCK_DEV1', 'F0013')}}
),
final as(
    select  {{dbt_utils.surrogate_key(['src_sys_nm', 'src_currency_cd'])}} as opco_currency_sk, 
            {{dbt_utils.surrogate_key(['src_sys_nm', 'src_currency_cd', 'src_currency_nm'])}} as opco_currency_ck,
            *
    from jde_hnck_opco_currency
),
delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'JDE_HNCK_OPCO_CURRENCY') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CURRENCY') and _load != 3%}
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
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_currency where src_sys_nm = 'JDE-HNCK')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_currency_ck not in (select distinct opco_currency_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_currency where src_sys_nm = 'JDE-HNCK')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_currency_ck in (select distinct opco_currency_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_currency where src_sys_nm = 'JDE-HNCK') and delete_dtm is not null)
    {% endif %}
{% endif %}