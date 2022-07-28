{% set _load = load_type('AX_OPCO_ITEM_SIZE') %}

with ax_opco_item_size as(
    select 
    current_timestamp as crt_dtm,
    its._fivetran_synced as stg_load_dtm,
    case
        when its._fivetran_deleted = true then its._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(its .inventsizeid) as src_item_size_cd,
    its.dataareaid as opco_id,
    its.itemid as src_item_cd,
    oi.opco_item_sk as src_item_sk,
    its.name as src_item_size_nm,
    its.description as src_item_size_desc
    from {{source('AX_DEV', 'INVENTSIZE')}} its 
    left join {{ref('opco_item')}} oi 
        on its.dataareaid = oi.opco_id
        and its.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    where dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select distinct 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_item_size_cd', 'src_item_cd']) }} as opco_item_size_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_item_size_cd', 'opco_id', 'src_item_cd', 'src_item_sk', 'src_item_size_nm', 'src_item_size_desc']) }} as opco_item_size_ck,
            * 
    from ax_opco_item_size 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_ITEM_SIZE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_ITEM_SIZE') and _load != 3%}
    union
    select
    OPCO_ITEM_SIZE_SK, OPCO_ITEM_SIZE_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, src_item_size_cd, opco_id, src_item_cd, src_item_sk, src_item_size_nm, src_item_size_desc
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_SIZE
    {% if _load == 1 %}
        where OPCO_ITEM_SIZE_ck not in (select distinct OPCO_ITEM_SIZE_ck from final)
    {% else %}
        where OPCO_ITEM_SIZE_ck not in (select distinct OPCO_ITEM_SIZE_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_SIZE where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_SIZE_ck not in (select distinct OPCO_ITEM_SIZE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_SIZE where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_SIZE_ck in (select distinct OPCO_ITEM_SIZE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_SIZE where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  