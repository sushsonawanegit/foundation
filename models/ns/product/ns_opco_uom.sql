{% set _load = load_type('NS_OPCO_UOM') %}

with ns_opco_uom as (
    select 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    uom_id::varchar(15) as src_uom_cd,
    name::varchar(50) as src_uom_desc
    from {{source('NS_DEV', 'UOM')}}
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_uom_cd']) }} as opco_uom_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_uom_cd', 'src_uom_desc']) }} as opco_uom_ck,
            * 
    from ns_opco_uom
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'NS_OPCO_UOM') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_UOM') and _load != 3 %}
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
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_UOM where src_sys_nm = 'NS')
    {% endif %} 
    and src_sys_nm = 'NS'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_UOM_ck not in (select distinct OPCO_UOM_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_UOM where src_sys_nm = 'NS')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_UOM_ck in (select distinct OPCO_UOM_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_UOM where src_sys_nm = 'NS') and delete_dtm is not null)
    {% endif %}
{% endif %}  