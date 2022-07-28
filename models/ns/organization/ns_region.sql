{% set _load = load_type('NS_REGION') %}

with region as (
    SELECT 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    region_opi_id::varchar(15) as region_id,
    region_opi_name::varchar(50) as region_nm,
    case 
        when is_inactive = 'F' then 1
        when is_inactive = 'T' then 0
    end::numeric(1,0) as actv_ind
    from {{source('NS_DEV', 'REGION_OPI')}}
),
final as(
    select {{ dbt_utils.surrogate_key(['src_sys_nm', 'region_id']) }} as region_sk,
    {{ dbt_utils.surrogate_key(['src_sys_nm', 'region_id','region_nm','actv_ind']) }} as region_ck, * 
    from region  
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'NS_REGION') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'REGION') and _load != 3%}
    union
    select
    REGION_SK, REGION_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    SRC_SYS_NM, region_id, region_nm, actv_ind
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.REGION
    {% if _load == 1 %}
        where REGION_ck not in (select distinct REGION_ck from final)
    {% else %}
        where REGION_ck not in (select distinct REGION_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.REGION where src_sys_nm = 'NS')
    {% endif %} 
    and src_sys_nm = 'NS'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and REGION_ck not in (select distinct REGION_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.REGION where src_sys_nm = 'NS')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and REGION_ck in (select distinct REGION_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.REGION where src_sys_nm = 'NS') and delete_dtm is not null)
    {% endif %}
{% endif %}