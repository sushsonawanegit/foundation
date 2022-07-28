{% set _load = load_type('NS_OPCO_PYMNT_TERMS') %}

with ns_opco_pymnt_terms as(
    select
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    payment_terms_id::varchar(40) as src_pymnt_terms_cd,
    name::varchar(100) as src_pymnt_terms_desc
    from {{source('NS_DEV', 'PAYMENT_TERMS')}}
),
final as(
    select  {{dbt_utils.surrogate_key(['src_sys_nm', 'src_pymnt_terms_cd'])}} as opco_pymnt_terms_sk, 
            {{dbt_utils.surrogate_key(['src_sys_nm', 'src_pymnt_terms_cd','src_pymnt_terms_desc'])}} as opco_pymnt_terms_ck, *
    from ns_opco_pymnt_terms   
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'NS_OPCO_PYMNT_TERMS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PYMNT_TERMS') and _load != 3%}
    union
    select
    OPCO_pymnt_terms_SK, OPCO_pymnt_terms_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    SRC_SYS_NM, SRC_pymnt_terms_CD, src_pymnt_terms_desc  
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_pymnt_terms
    {% if _load == 1 %}
        where opco_pymnt_terms_ck not in (select distinct opco_pymnt_terms_ck from final)
    {% else %}
        where opco_pymnt_terms_ck not in (select distinct opco_pymnt_terms_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_pymnt_terms where src_sys_nm = 'NS')
    {% endif %} 
    and src_sys_nm = 'NS'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_pymnt_terms_ck not in (select distinct opco_pymnt_terms_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_pymnt_terms where src_sys_nm = 'NS')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_pymnt_terms_ck in (select distinct opco_pymnt_terms_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_pymnt_terms where src_sys_nm = 'NS') and delete_dtm is not null)
    {% endif %}
{% endif %}