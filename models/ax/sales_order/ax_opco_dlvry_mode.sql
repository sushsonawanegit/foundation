{% set _load = load_type('AX_OPCO_DLVRY_MODE') %}

with ax_opco_dlvry_mode as(
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(code) as src_dlvry_mode_cd,
    max(txt) as src_dlvry_mode_desc
    from {{source('AX_DEV', 'DLVMODE')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_dlvry_mode_cd, _fivetran_deleted
),
final as(
    select 
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_dlvry_mode_cd']) }} as opco_dlvry_mode_sk,
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_dlvry_mode_cd','src_dlvry_mode_desc']) }} as opco_dlvry_mode_ck, 
        *
    from ax_opco_dlvry_mode
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_DLVRY_MODE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_DLVRY_MODE') and _load != 3%}
    union
    select
    opco_dlvry_mode_sk, opco_dlvry_mode_ck, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, src_dlvry_mode_cd, src_dlvry_mode_desc
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_DLVRY_MODE
    {% if _load == 1 %}
        where opco_dlvry_mode_ck not in (select distinct opco_dlvry_mode_ck from final)
    {% else %}
        where opco_dlvry_mode_ck not in (select distinct opco_dlvry_mode_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_DLVRY_MODE where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_dlvry_mode_ck not in (select distinct opco_dlvry_mode_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_DLVRY_MODE where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_dlvry_mode_ck in (select distinct opco_dlvry_mode_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_DLVRY_MODE where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}