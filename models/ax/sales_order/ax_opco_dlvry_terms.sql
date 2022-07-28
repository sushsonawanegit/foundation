{% set _load = load_type('AX_OPCO_DLVRY_TERMS') %}

with ax_opco_dlvry_terms as (
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(code) as src_dlvry_terms_cd,
    max(txt) as src_dlvry_terms_desc
    from {{source('AX_DEV', 'DLVTERM')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_dlvry_terms_cd, _fivetran_deleted
),
final as(
    select 
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_dlvry_terms_cd']) }} as opco_dlvry_terms_sk, 
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_dlvry_terms_cd','src_dlvry_terms_desc']) }} as opco_dlvry_terms_ck, 
        *
    from ax_opco_dlvry_terms
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_DLVRY_TERMS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_DLVRY_TERMS') and _load != 3%}
    union
    select
    opco_dlvry_terms_sk, opco_dlvry_terms_ck, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, src_dlvry_terms_cd, src_dlvry_terms_desc
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_DLVRY_TERMS
    {% if _load == 1 %}
        where opco_dlvry_terms_ck not in (select distinct opco_dlvry_terms_ck from final)
    {% else %}
        where opco_dlvry_terms_ck not in (select distinct opco_dlvry_terms_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_DLVRY_TERMS where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_dlvry_terms_ck not in (select distinct opco_dlvry_terms_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_DLVRY_TERMS where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_dlvry_terms_ck in (select distinct opco_dlvry_terms_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_DLVRY_TERMS where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}