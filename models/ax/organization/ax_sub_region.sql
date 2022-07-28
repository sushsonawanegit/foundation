{% set _load = load_type('AX_SUB_REGION') %}

with ax_sub_region as(
    SELECT DISTINCT
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    upper(subregion_opi)::varchar(20) as sub_region_id,
    subregion_opi::varchar(20) as sub_region_nm,
    1::numeric(1,0) as actv_ind
    from {{source('AX_DEV', 'COMPANYINFO')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'sub_region_id']) }} as sub_region_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'sub_region_id', 'sub_region_nm', 'actv_ind']) }} as sub_region_ck,
            * 
    from ax_sub_region    
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_SUB_REGION') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'SUB_REGION') and _load != 3%}
    union
    select
    SUB_REGION_SK, SUB_REGION_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, sub_region_id, sub_region_nm, actv_ind
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.SUB_REGION
    {% if _load == 1 %}
        where SUB_REGION_ck not in (select distinct SUB_REGION_ck from final)
    {% else %}
        where SUB_REGION_ck not in (select distinct SUB_REGION_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.SUB_REGION where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and SUB_REGION_ck not in (select distinct SUB_REGION_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.SUB_REGION where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and SUB_REGION_ck in (select distinct SUB_REGION_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.SUB_REGION where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  