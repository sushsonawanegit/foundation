{% set _load = load_type('SYSPRO_PRIMEX_OPCO_TYPE') %}

with syspro_primex_opco_type as(
    {% for sch in var('primex_schemas') %}
        {% if tb_check(var('primex_db'), sch, 'CUSGENMASTER_')  %}
            select
            substr('{{sch}}', 22, 1) as src_comp,
            current_timestamp as crt_dtm,
            mule_load_ts as stg_load_dtm,
            null::timestamp_tz as delete_dtm,
            'SYSPRO-PRMX' as src_sys_nm,
            upper(trim(type)) as src_type_cd,
            trim(type) as src_type_desc,
            1::number(1,0) as actv_ind,
            {{ spcl_chr_rp('src_type_desc')}} as type_wo_spcl_chr_cd
            from {{ var('primex_db')}}.{{ sch}}.CUSGENMASTER_
            {% if not loop.last %} union all {% endif %} 
        {% endif %}
    {% endfor %}
),
de_duplication as(
    select *
           , rank() over(partition by src_type_cd order by stg_load_dtm, src_comp) as rnk
    from syspro_primex_opco_type
),
final as(
    select distinct
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_type_cd']) }} as opco_type_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_type_cd', 'src_type_desc', 'actv_ind', 'type_wo_spcl_chr_cd']) }} as opco_type_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_type_cd, src_type_desc, actv_ind, type_wo_spcl_chr_cd
    from de_duplication
    where rnk = 1 
),

delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'SYSPRO_PRIMEX_OPCO_TYPE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_TYPE') and _load != 3%}
        union
        select
        OPCO_TYPE_SK, OPCO_TYPE_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        SRC_SYS_NM, src_type_cd, src_type_desc, actv_ind, type_wo_spcl_chr_cd 
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE
        {% if _load == 1 %}
            where OPCO_TYPE_ck not in (select distinct OPCO_TYPE_ck from final)
        {% else %}
            where OPCO_TYPE_ck not in (select distinct OPCO_TYPE_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE where src_sys_nm = 'SYSPRO-PRMX')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_TYPE_ck not in (select distinct OPCO_TYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_TYPE_ck in (select distinct OPCO_TYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    {% endif %}
{% endif %}