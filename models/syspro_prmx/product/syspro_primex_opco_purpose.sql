{% set _load = load_type('SYSPRO_PRIMEX_OPCO_PURPOSE') %}

with syspro_primex_opco_purpose as(
    {% set tables = table_check('COMPANY', '_DBO_CUSGENMASTER_') %}
    {% for tbl in tables %}
        select
        '{{ tbl}}' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(purpose)) as src_purpose_cd,
        trim(purpose) as src_purpose_desc,
        1::number(1,0) as actv_ind,
        {{ spcl_chr_rp('src_purpose_desc')}} as purpose_wo_spcl_chr_cd
        from {{ var('primex_db')}}.{{ var('primex_schema')}}.COMPANY{{ tbl}}_DBO_CUSGENMASTER_
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
),
de_duplication as(
    select *
           , rank() over(partition by src_purpose_cd order by stg_load_dtm, src_comp) as rnk
    from syspro_primex_opco_purpose
),
final as(
    select distinct
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_purpose_cd']) }} as opco_purpose_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_purpose_cd', 'src_purpose_desc','actv_ind', 'purpose_wo_spcl_chr_cd']) }} as opco_purpose_ck, 
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_purpose_cd, src_purpose_desc, actv_ind, purpose_wo_spcl_chr_cd
    from de_duplication
    where rnk = 1 
),
delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'SYSPRO_PRIMEX_OPCO_PURPOSE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PURPOSE') and _load != 3%}
        union
        select
        OPCO_PURPOSE_SK, OPCO_PURPOSE_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, src_purpose_cd, src_purpose_desc, actv_ind, purpose_wo_spcl_chr_cd  
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE
        {% if _load == 1 %}
            where OPCO_PURPOSE_ck not in (select distinct OPCO_PURPOSE_ck from final)
        {% else %}
            where OPCO_PURPOSE_ck not in (select distinct OPCO_PURPOSE_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE where src_sys_nm = 'SYSPRO-PRMX')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PURPOSE_ck not in (select distinct OPCO_PURPOSE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PURPOSE_ck in (select distinct OPCO_PURPOSE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    {% endif %}
{% endif %}