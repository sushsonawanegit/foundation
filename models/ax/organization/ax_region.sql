{% set _load = load_type('AX_REGION') %}

with ax_region as (
    SELECT DISTINCT
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    upper(region_opi)::varchar(25) as region_id,
    region_opi::varchar(50) as region_nm,
    1::numeric(1,0) as actv_ind
    from {{source('AX_DEV', 'COMPANYINFO')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
),
final as(
    select {{ dbt_utils.surrogate_key(['src_sys_nm', 'region_id']) }} as region_sk, 
           {{ dbt_utils.surrogate_key(['src_sys_nm', 'region_id', 'region_nm', 'actv_ind']) }} as region_ck,
           * 
    from ax_region
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_REGION') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'REGION') and _load != 3%}
        union
        select
        region_sk, region_ck, crt_dtm, 
        (select MAX(STG_LOAD_DTM) from final) as stg_load_dtm,
        (select MAX(STG_LOAD_DTM) from final) as delete_dtm,
        src_sys_nm, region_id, region_nm, actv_ind
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.REGION
        {% if _load == 1 %}
            where REGION_ck not in (select distinct REGION_ck from final)
        {% else %}
            where REGION_ck not in (select distinct REGION_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.REGION where src_sys_nm = 'AX')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and REGION_ck not in (select distinct REGION_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.REGION where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and REGION_ck in (select distinct REGION_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.REGION where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  