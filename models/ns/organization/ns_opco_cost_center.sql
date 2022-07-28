{% set _load = load_type('NS_OPCO_COST_CENTER') %}

with ns_opco_cost_center as (
    select 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    location_id::varchar(20) as src_cost_center_cd,
    name::varchar(100) as src_cost_center_desc,
    {{ spcl_chr_rp('src_cost_center_desc')}}::varchar(100) as cost_center_wo_spcl_chr_cd,
    case
        when isinactive = 'Yes' then 0
        else 1
    end::numeric(1,0) as actv_ind,
    null::varchar(50) as src_cost_center_type_txt
    from {{source('NS_DEV', 'LOCATIONS')}}  
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_cost_center_cd']) }} as opco_cost_center_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_cost_center_cd', 'src_cost_center_desc']) }} as opco_cost_center_ck,
            *
    from ns_opco_cost_center 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'NS_OPCO_COST_CENTER') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_COST_CENTER') and _load != 3%}
    union
    select
    OPCO_COST_CENTER_SK, OPCO_COST_CENTER_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, src_cost_center_cd, src_cost_center_desc
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER
    {% if _load == 1 %}
        where OPCO_COST_CENTER_ck not in (select distinct OPCO_COST_CENTER_ck from final)
    {% else %}
        where OPCO_COST_CENTER_ck not in (select distinct OPCO_COST_CENTER_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER where src_sys_nm = 'NS')
    {% endif %} 
    and src_sys_nm = 'NS'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_COST_CENTER_ck not in (select distinct OPCO_COST_CENTER_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER where src_sys_nm = 'NS')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_COST_CENTER_ck in (select distinct OPCO_COST_CENTER_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER where src_sys_nm = 'NS') and delete_dtm is not null)
    {% endif %}
{% endif %}