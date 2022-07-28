{% set _load = load_type('AX_OPCO_ITEM_FREIGHT') %}

with ax_opco_item_freight as (
    select 
    current_timestamp as crt_dtm,
    it._fivetran_synced as stg_load_dtm,
    case
        when it._fivetran_deleted = true then it._fivetran_synced 
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(it.itemgroupid) as src_item_freight_cd,
    it.dataareaid as opco_id,
    it.itemname as src_item_freight_nm
    from {{source('AX_DEV', 'INVENTTABLE')}} it
    join {{source('AX_DEV', 'INVENTITEMGROUP')}} itg 
        on it.dataareaid = itg.dataareaid
        and it.itemid = itg.ifitemid_opi
        and itg._fivetran_deleted = false
    where it.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select distinct 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_item_freight_cd']) }} as opco_item_freight_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_item_freight_cd','src_item_freight_nm']) }} as OPCO_ITEM_FREIGHT_ck,
            * 
    from ax_opco_item_freight
)
    
select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_ITEM_FREIGHT') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_ITEM_FREIGHT') and _load != 3%}
    union
    select
    OPCO_ITEM_FREIGHT_SK, OPCO_ITEM_FREIGHT_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, src_item_freight_cd, opco_id, src_item_freight_nm
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_FREIGHT
    {% if _load == 1 %}
        where OPCO_ITEM_FREIGHT_ck not in (select distinct OPCO_ITEM_FREIGHT_ck from final)
    {% else %}
        where OPCO_ITEM_FREIGHT_ck not in (select distinct OPCO_ITEM_FREIGHT_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_FREIGHT where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_FREIGHT_ck not in (select distinct OPCO_ITEM_FREIGHT_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_FREIGHT where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_FREIGHT_ck in (select distinct OPCO_ITEM_FREIGHT_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_FREIGHT where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}