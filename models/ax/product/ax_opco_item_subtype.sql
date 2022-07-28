{% set _load = load_type('AX_OPCO_ITEM_SUBTYPE') %}

with ax_opco_item_subtype as(
    select
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    code_opi as src_item_subtype_cd,
    upper(category_opi) as src_item_type_cd,
    max(description_opi) as src_item_subtype_desc
    from {{source('AX_DEV', 'INVENTPRECASTCODE_OPI')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_item_type_cd, src_item_subtype_cd, _fivetran_deleted
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_item_type_cd', 'src_item_subtype_cd']) }} as OPCO_ITEM_SUBTYPE_SK,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_item_type_cd', 'src_item_subtype_cd', 'src_item_subtype_desc']) }} as OPCO_ITEM_SUBTYPE_ck, 
            * 
    from ax_opco_item_subtype 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_ITEM_SUBTYPE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_ITEM_SUBTYPE') and _load != 3%}
    union
    select
    OPCO_ITEM_SUBTYPE_SK, OPCO_ITEM_SUBTYPE_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm,  src_item_type_cd,  src_item_subtype_cd,  src_item_subtype_desc
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_SUBTYPE
    {% if _load == 1 %}
        where OPCO_ITEM_SUBTYPE_ck not in (select distinct OPCO_ITEM_SUBTYPE_ck from final)
    {% else %}
        where OPCO_ITEM_SUBTYPE_ck not in (select distinct OPCO_ITEM_SUBTYPE_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_SUBTYPE where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_SUBTYPE_ck not in (select distinct OPCO_ITEM_SUBTYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_SUBTYPE where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_SUBTYPE_ck in (select distinct OPCO_ITEM_SUBTYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_SUBTYPE where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  