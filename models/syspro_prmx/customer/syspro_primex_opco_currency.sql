{% set _load = load_type('SYSPRO_PRIMEX_OPCO_CURRENCY') %}

with syspro_primex_opco_currency as(
    {% for sch in var(('primex_schemas')) %}
        {% if tb_check(var('primex_db'), sch, 'TBLCURRENCY')  %}
            select 
            substr('{{sch}}', 22, 1) as src_comp,
            current_timestamp as crt_dtm,
            mule_load_ts as stg_load_dtm, 
            null::timestamp_tz as delete_dtm,
            'SYSPRO-PRMX'::varchar as src_sys_nm,
            upper(trim(currency)) as src_currency_cd,
            trim(description) as src_currency_nm
            from {{ var('primex_db')}}.{{ sch}}.TBLCURRENCY
            {% if not loop.last %} union all {% endif %}
        {% endif %}
    {% endfor %}
),
de_duplication as(
    select *
           , rank() over(partition by src_currency_cd order by stg_load_dtm, src_comp) as rnk
    from syspro_primex_opco_currency
),
final as(
    select distinct
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_currency_cd']) }} as opco_currency_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_currency_cd', 'src_currency_nm']) }} as opco_currency_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_currency_cd, src_currency_nm
    from de_duplication
    where rnk = 1
),
delete_captured as (
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'SYSPRO_PRIMEX_OPCO_CURRENCY') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CURRENCY') and _load != 3%}
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
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_currency where src_sys_nm = 'SYSPRO-PRMX')
        {% endif %} 
        and src_sys_nm = 'SYSPRO-PRMX'
    {% endif %}
)

select * from delete_captured


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_currency_ck not in (select distinct opco_currency_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_currency where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_currency_ck in (select distinct opco_currency_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_currency where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    {% endif %}
{% endif %}