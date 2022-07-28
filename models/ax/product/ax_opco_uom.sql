{% set _load = load_type('AX_OPCO_UOM') %}

with ax_opco_uom as (
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    upper(unitid)::varchar(15) as src_uom_cd,
    max(txt)::varchar(50) as src_uom_desc
    from {{source('AX_DEV', 'UNIT')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_uom_cd, _fivetran_deleted
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_uom_cd']) }} as opco_uom_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_uom_cd', 'src_uom_desc']) }} as opco_uom_ck,
            * 
    from ax_opco_uom
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_UOM') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_UOM') and _load != 3%}
        union
        select
        OPCO_UOM_SK, OPCO_UOM_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, src_uom_cd, src_uom_desc
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_UOM
        {% if _load == 1 %}
            where OPCO_UOM_ck not in (select distinct OPCO_UOM_ck from final)
        {% else %}
            where OPCO_UOM_ck not in (select distinct OPCO_UOM_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_UOM where src_sys_nm = 'AX')
        {% endif %} 
        and src_sys_nm = 'AX'
    {% endif %}
)
select * from final


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_UOM_ck not in (select distinct OPCO_UOM_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_UOM where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_UOM_ck in (select distinct OPCO_UOM_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_UOM where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  