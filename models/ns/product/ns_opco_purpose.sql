{% set _load = load_type('NS_OPCO_PURPOSE') %}

with ns_opco_purpose as(
    select 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    upper(market_opi_id)::varchar(20) as src_purpose_cd,
    market_opi_name::varchar(100) as src_purpose_desc,
    case 
        when is_inactive = 'T' then 0 
        else 1 
    end::numeric(1,0) as actv_ind,
    {{ spcl_chr_rp('src_purpose_desc')}}::varchar(100) as purpose_wo_spcl_chr_cd
    from {{source('NS_DEV', 'MARKET_OPI')}}
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_purpose_cd']) }} as opco_purpose_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_purpose_cd', 'src_purpose_desc', 'purpose_wo_spcl_chr_cd', 'actv_ind']) }} as opco_purpose_ck,
            * 
    from ns_opco_purpose 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'NS_OPCO_PURPOSE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PURPOSE') and _load != 3%}
    union
    select
    OPCO_PURPOSE_SK, OPCO_PURPOSE_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, src_purpose_cd, src_purpose_desc, actv_ind, purpose_wo_spcl_chr_cd
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE
    {% if _load == 1 %}
        where OPCO_PURPOSE_ck not in (select distinct OPCO_PURPOSE_ck from final)
    {% else %}
        where OPCO_PURPOSE_ck not in (select distinct OPCO_PURPOSE_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE where src_sys_nm = 'NS')
    {% endif %} 
    and src_sys_nm = 'NS'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PURPOSE_ck not in (select distinct OPCO_PURPOSE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE where src_sys_nm = 'NS')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PURPOSE_ck in (select distinct OPCO_PURPOSE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE where src_sys_nm = 'NS') and delete_dtm is not null)
    {% endif %}
{% endif %}  