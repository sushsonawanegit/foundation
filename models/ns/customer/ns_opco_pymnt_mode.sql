{% set _load = load_type('NS_OPCO_PYMNT_MODE') %}

with ns_opco_pymnt_mode as(
    select 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    payment_method_id::varchar(10) as src_pymnt_mode_cd,
    name::varchar(100) as src_pymnt_mode_desc
    from {{source('NS_DEV', 'PAYMENT_METHODS')}}
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_pymnt_mode_cd']) }} as opco_pymnt_mode_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_pymnt_mode_cd','src_pymnt_mode_desc']) }} as opco_pymnt_mode_ck,
            * 
    from ns_opco_pymnt_mode
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'NS_OPCO_PYMNT_MODE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PYMNT_MODE') and _load != 3 %}
    union
    select
    OPCO_pymnt_mode_SK, OPCO_pymnt_mode_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    SRC_SYS_NM, SRC_pymnt_mode_CD, src_pymnt_mode_desc  
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PYMNT_MODE
    {% if _load == 1 %}
        where opco_pymnt_mode_ck not in (select distinct opco_pymnt_mode_ck from final)
    {% else %}
        where opco_pymnt_mode_ck not in (select distinct opco_pymnt_mode_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PYMNT_MODE where src_sys_nm = 'NS')
    {% endif %} 
    and src_sys_nm = 'NS'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_pymnt_mode_ck not in (select distinct opco_pymnt_mode_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PYMNT_MODE where src_sys_nm = 'NS')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_pymnt_mode_ck in (select distinct opco_pymnt_mode_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PYMNT_MODE where src_sys_nm = 'NS') and delete_dtm is not null)
    {% endif %}
{% endif %}