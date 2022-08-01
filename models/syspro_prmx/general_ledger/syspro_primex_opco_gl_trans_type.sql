{% set _load = load_type('SYSPRO_PRIMEX_OPCO_GL_TRANS_TYPE') %}

with syspro_primex_opco_gl_trans_type as(
    {% set tables = table_check('COMPANY', '_DBO_GENJOURNALCTL') %}
    {% for tbl in tables %}
        select
        '{{ tbl}}' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(journalsource)) as src_gl_trans_type_cd,
        case 
            when trim(journalsource) = 'AP' then 'Accounts Payable'
            when trim(journalsource) = 'AR' then 'Accounts Receivable'
            when trim(journalsource) = 'CS' then 'Cash'
            when trim(journalsource) = 'GR' then 'Goods Receipt'
            when trim(journalsource) = 'IN' then 'Inventory'
            when trim(journalsource) = 'SA' then 'Sales'
            when trim(journalsource) = 'WP' then 'WIP Labour'
            else trim(journalsource)
        end as src_gl_trans_type_desc
        from {{ var('primex_db')}}.{{ var('primex_schema')}}.COMPANY{{ tbl}}_DBO_GENJOURNALCTL
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
),
de_duplication as(
    select *
           , rank() over(partition by src_gl_trans_type_cd order by stg_load_dtm, src_comp) as rnk
    from syspro_primex_opco_gl_trans_type
),
final as(
    select distinct
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_gl_trans_type_cd']) }} as opco_gl_trans_type_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_gl_trans_type_cd','src_gl_trans_type_desc']) }} as opco_gl_trans_type_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_gl_trans_type_cd, src_gl_trans_type_desc
    from de_duplication
    where rnk = 1 
),

delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'SYSPRO_PRIMEX_OPCO_GL_TRANS_TYPE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_GL_TRANS_TYPE') and _load != 3%}
        union
        select
        OPCO_GL_TRANS_TYPE_SK, OPCO_GL_TRANS_TYPE_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        SRC_SYS_NM, src_gl_trans_type_cd, src_gl_trans_type_desc  
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS_TYPE
        {% if _load == 1 %}
            where OPCO_GL_TRANS_TYPE_ck not in (select distinct OPCO_GL_TRANS_TYPE_ck from final)
        {% else %}
            where OPCO_GL_TRANS_TYPE_ck not in (select distinct OPCO_GL_TRANS_TYPE_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS_TYPE where src_sys_nm = 'SYSPRO-PRMX')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_GL_TRANS_TYPE_ck not in (select distinct OPCO_GL_TRANS_TYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS_TYPE where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_GL_TRANS_TYPE_ck in (select distinct OPCO_GL_TRANS_TYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS_TYPE where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    {% endif %}
{% endif %}