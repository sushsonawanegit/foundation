{% set _load = load_type('NS_OPCO_BRAND') %}

with ns_opco_brand as(
    select 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    upper(class_id)::varchar(20) as src_brand_cd,
    name::varchar(100) as src_brand_nm,
    case 
        when isinactive = 'Yes' then 0 
        else 1 
    end::numeric(1,0) as actv_ind,
    {{ spcl_chr_rp('src_brand_nm')}}::varchar(100) as brand_wo_spcl_chr_cd
    from {{source('NS_DEV', 'CLASSES')}}
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_brand_cd']) }} as opco_brand_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_brand_cd', 'src_brand_nm', 'brand_wo_spcl_chr_cd', 'actv_ind']) }} as opco_brand_ck,
            *
    from ns_opco_brand 
)

select *  from final 
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'NS_OPCO_BRAND') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_BRAND') and _load != 3%}
    union
    select
    OPCO_BRAND_SK, OPCO_BRAND_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, src_brand_cd, src_brand_nm, actv_ind, brand_wo_spcl_chr_cd
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_BRAND
    {% if _load == 1 %}
        where OPCO_BRAND_ck not in (select distinct OPCO_BRAND_ck from final)
    {% else %}
        where OPCO_BRAND_ck not in (select distinct OPCO_BRAND_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_BRAND where src_sys_nm = 'NS')
    {% endif %} 
    and src_sys_nm = 'NS'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_BRAND_ck not in (select distinct OPCO_BRAND_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_BRAND where src_sys_nm = 'NS')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_BRAND_ck in (select distinct OPCO_BRAND_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_BRAND where src_sys_nm = 'NS') and delete_dtm is not null)
    {% endif %}
{% endif %}  