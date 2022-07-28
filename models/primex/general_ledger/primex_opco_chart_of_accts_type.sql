{% set _load = load_type('PRIMEX_OPCO_CHART_OF_ACCTS_TYPE') %}

with primex_opco_chart_of_accts_type as(
    {% set tables = table_check('COMPANY', '_DBO_GENGROUPS') %}
    {% for tbl in tables %}
        select
        '{{ tbl}}' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(glgroup)) as src_acct_type_cd,
        trim(description) as src_acct_type_desc
        from {{ var('primex_db')}}.{{ var('primex_schema')}}.COMPANY{{ tbl}}_DBO_GENGROUPS
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
),
de_duplication as(
    select *
           , rank() over(partition by src_acct_type_cd order by src_comp) as rnk
    from primex_opco_chart_of_accts_type
),
final as(
    select distinct
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_acct_type_cd']) }} as opco_chart_of_accts_type_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_acct_type_cd','src_acct_type_desc']) }} as opco_chart_of_accts_type_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_acct_type_cd, src_acct_type_desc       
    from de_duplication
    where rnk = 1 
),
delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'PRIMEX_OPCO_CHART_OF_ACCTS_TYPE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CHART_OF_ACCTS_TYPE') and _load != 3%}
        union
        select
        OPCO_CHART_OF_ACCTS_TYPE_SK, OPCO_CHART_OF_ACCTS_TYPE_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, src_acct_type_cd, src_acct_type_desc   
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS_TYPE
        {% if _load == 1 %}
            where OPCO_CHART_OF_ACCTS_TYPE_ck not in (select distinct OPCO_CHART_OF_ACCTS_TYPE_ck from final)
        {% else %}
            where OPCO_CHART_OF_ACCTS_TYPE_ck not in (select distinct OPCO_CHART_OF_ACCTS_TYPE_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS_TYPE where src_sys_nm = 'SYSPRO-PRMX')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_CHART_OF_ACCTS_TYPE_ck not in (select distinct OPCO_CHART_OF_ACCTS_TYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS_TYPE where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_CHART_OF_ACCTS_TYPE_ck in (select distinct OPCO_CHART_OF_ACCTS_TYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CHART_OF_ACCTS_TYPE where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    {% endif %}
{% endif %}