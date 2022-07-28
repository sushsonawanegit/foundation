{% set _load = load_type('AX_OPCO_ITEM_CLASS') %}

with ax_opco_item_class as(
    select
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(inventproductclassid_opi) as src_item_class_cd,
    max(name_opi) as src_item_class_desc
    from {{source('AX_DEV', 'INVENTPRODUCTCLASS_OPI')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_item_class_cd, _fivetran_deleted 
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_item_class_cd']) }} as opco_item_class_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_item_class_cd', 'src_item_class_desc']) }} as opco_item_class_ck,
            * 
    from ax_opco_item_class
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_ITEM_CLASS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_ITEM_CLASS') and _load != 3%}
    union
    select
    OPCO_ITEM_CLASS_SK, OPCO_ITEM_CLASS_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, src_ITEM_CLASS_cd, src_ITEM_CLASS_desc
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_CLASS
    {% if _load == 1 %}
        where OPCO_ITEM_CLASS_ck not in (select distinct OPCO_ITEM_CLASS_ck from final)
    {% else %}
        where OPCO_ITEM_CLASS_ck not in (select distinct OPCO_ITEM_CLASS_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_CLASS where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}
   

{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_CLASS_ck not in (select distinct OPCO_ITEM_CLASS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_CLASS where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_CLASS_ck in (select distinct OPCO_ITEM_CLASS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_CLASS where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %} 