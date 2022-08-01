{% set _load = load_type('SYSPRO_PRIMEX_OPCO_COST_CENTER') %}

with syspro_primex_opco_cost_center as(
    {% set tables = table_check('COMPANY', '_DBO_CUSGENMASTER_') %}
    {% for tbl in tables %}
        select
        '{{ tbl}}' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(cc)) as src_cost_center_cd,
        trim(cc) as src_cost_center_desc,
        1::number(1,0) as actv_ind,
        null as src_cost_center_type_txt,
        {{ spcl_chr_rp('src_cost_center_desc')}} as cost_center_wo_spcl_chr_cd
        from {{ var('primex_db')}}.{{ var('primex_schema')}}.COMPANY{{ tbl}}_DBO_CUSGENMASTER_
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
),
de_duplication as(
    select *
           , rank() over(partition by src_cost_center_cd order by stg_load_dtm, src_comp) as rnk
    from syspro_primex_opco_cost_center
),
final as(
    select distinct
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_cost_center_cd']) }} as opco_cost_center_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_cost_center_cd', 'src_cost_center_desc', 'actv_ind', 'src_cost_center_type_txt', 'cost_center_wo_spcl_chr_cd']) }} as opco_cost_center_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_cost_center_cd, src_cost_center_desc, actv_ind, src_cost_center_type_txt, cost_center_wo_spcl_chr_cd 
    from de_duplication
    where rnk = 1
),
delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'SYSPRO_PRIMEX_OPCO_COST_CENTER') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_COST_CENTER') and _load != 3%}
        union
        select
        OPCO_COST_CENTER_SK, OPCO_COST_CENTER_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        SRC_SYS_NM, src_cost_center_cd, src_cost_center_desc, actv_ind, src_cost_center_type_txt, cost_center_wo_spcl_chr_cd 
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER
        {% if _load == 1 %}
            where OPCO_COST_CENTER_ck not in (select distinct OPCO_COST_CENTER_ck from final)
        {% else %}
            where OPCO_COST_CENTER_ck not in (select distinct OPCO_COST_CENTER_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER where src_sys_nm = 'SYSPRO-PRMX')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_COST_CENTER_ck not in (select distinct OPCO_COST_CENTER_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_COST_CENTER_ck in (select distinct OPCO_COST_CENTER_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    {% endif %}
{% endif %}