{% set _load = load_type('JDE_HNCK_OPCO_UOM') %}

with jde_hnck_opco_uom as (
    select 
    current_timestamp as crt_dtm,
    null::timestamp_tz as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'JDE_HNCK'::varchar(20) as src_sys_nm,
    upper(trim(drky))::varchar(15) as src_uom_cd,
    trim(drdl01)::varchar(50) as src_uom_desc
    from {{source('JDE_DEV', 'JDE_PRODUCTION_PRODCTL_F0005')}}
    where trim(drsy) = '00' and trim(drrt) = 'UM'		
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_uom_cd']) }} as opco_uom_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_uom_cd', 'src_uom_desc']) }} as opco_uom_ck,
            * 
    from jde_hnck_opco_uom
),
delete_captured as (
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'JDE_HNCK_OPCO_UOM') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_UOM') and _load != 3%}
        union
        select
        opco_uom_sk, opco_uom_ck, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        src_sys_nm, src_uom_cd, src_uom_desc
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_UOM
        {% if _load == 1 %}
            where OPCO_UOM_ck not in (select distinct OPCO_UOM_ck from final)
        {% else %}
            where OPCO_UOM_ck not in (select distinct OPCO_UOM_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_UOM where src_sys_nm = 'JDE-HNCK')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_UOM_ck not in (select distinct OPCO_UOM_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_UOM where src_sys_nm = 'JDE-HNCK')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_UOM_ck in (select distinct OPCO_UOM_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_UOM where src_sys_nm = 'JDE-HNCK') and delete_dtm is not null)
    {% endif %}
{% endif %}  