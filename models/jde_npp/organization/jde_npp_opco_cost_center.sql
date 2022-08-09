{% set _load = load_type('JDE_NPP_OPCO_COST_CENTER') %}

with jde_npp_opco_cost_center as (
    select 
    current_timestamp as crt_dtm,
    mule_load_ts as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'JDE-NPP'::varchar as src_sys_nm,
    upper(trim(mcmcu))::varchar as src_cost_center_cd,
    trim(mcdl01)::varchar as src_cost_center_desc,
    '1'::number(1,0) as actv_ind,
    trim(mcstyl)::varchar as src_cost_center_type_txt,
    {{ spcl_chr_rp('src_cost_center_desc')}}::varchar as cost_center_wo_spcl_chr_cd
    from {{source('JDE_NPP_DEV1', 'F0006')}}
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_cost_center_cd']) }} as opco_cost_center_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_cost_center_cd', 'actv_ind', 'src_cost_center_type_txt', 'cost_center_wo_spcl_chr_cd']) }} as opco_cost_center_ck,
            *
    from jde_npp_opco_cost_center
),
delete_captured as (
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'JDE_NPP_OPCO_COST_CENTER') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_COST_CENTER') and _load != 3%}
        union
        select
        OPCO_COST_CENTER_SK, OPCO_COST_CENTER_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, src_cost_center_cd, src_cost_center_desc, actv_ind, src_cost_center_type_txt, cost_center_wo_spcl_chr_cd
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER
        {% if _load == 1 %}
            where OPCO_COST_CENTER_ck not in (select distinct OPCO_COST_CENTER_ck from final)
        {% else %}
            where OPCO_COST_CENTER_ck not in (select distinct OPCO_COST_CENTER_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER where src_sys_nm = 'JDE-NPP')
        {% endif %} 
        and src_sys_nm = 'JDE-NPP'
    {% endif %}
)

select * from delete_captured


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_COST_CENTER_ck not in (select distinct OPCO_COST_CENTER_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER where src_sys_nm = 'JDE-NPP')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_COST_CENTER_ck in (select distinct OPCO_COST_CENTER_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER where src_sys_nm = 'JDE-NPP') and delete_dtm is not null)
    {% endif %}
{% endif %}