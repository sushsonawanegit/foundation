{% set _load = load_type('AX_WAREHOUSE') %}

with ax_warehouse as(
    SELECT
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case 
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    dataareaid as opco_id,
    upper(inventlocationid) as warehouse_id,
    name as warehouse_nm,
    1 as actv_ind
    from {{source('AX_DEV', 'INVENTLOCATION')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
),
final as(
    select {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'warehouse_id']) }} as warehouse_sk,
    {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'warehouse_id','warehouse_nm','actv_ind']) }} as warehouse_ck, 
    * 
    from ax_warehouse  
)


select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_WAREHOUSE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'WAREHOUSE') and _load != 3%}
    union
    select
    warehouse_sk, warehouse_ck, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, opco_id, warehouse_id, warehouse_nm, actv_ind 
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.WAREHOUSE
    {% if _load == 1 %}
        where warehouse_ck not in (select distinct warehouse_ck from final)
    {% else %}
        where warehouse_ck not in (select distinct warehouse_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.WAREHOUSE where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}

{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and warehouse_ck not in (select distinct warehouse_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.WAREHOUSE where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and warehouse_ck in (select distinct warehouse_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.WAREHOUSE where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}