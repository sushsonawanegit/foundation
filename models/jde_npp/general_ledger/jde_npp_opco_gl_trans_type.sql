{% set _load = load_type('JDE_NPP_OPCO_GL_TRANS_TYPE') %}

with jde_npp_opco_gl_trans_type as (
    select 
    current_timestamp as crt_dtm,
    mule_load_ts as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'JDE-NPP'::varchar as src_sys_nm,
    upper(trim(drky))::varchar as src_gl_trans_type_cd,
    trim(drdl01)::varchar as src_gl_trans_type_desc
    from {{source('JDE_NPP_DEV2', 'F0005')}}
    where trim(drsy) = '00' and drrt = 'DT'		
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_gl_trans_type_cd']) }} as opco_gl_trans_type_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_gl_trans_type_cd', 'src_gl_trans_type_desc']) }} as opco_gl_trans_type_ck,
            * 
    from jde_npp_opco_gl_trans_type
),

delete_captured as (
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'JDE_NPP_OPCO_GL_TRANS_TYPE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_GL_TRANS_TYPE') and _load != 3%}
        union
        select
        OPCO_GL_TRANS_TYPE_sk, OPCO_GL_TRANS_TYPE_ck, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        src_sys_nm, src_gl_trans_type_cd, src_gl_trans_type_desc
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS_TYPE
        {% if _load == 1 %}
            where OPCO_GL_TRANS_TYPE_ck not in (select distinct OPCO_GL_TRANS_TYPE_ck from final)
        {% else %}
            where OPCO_GL_TRANS_TYPE_ck not in (select distinct OPCO_GL_TRANS_TYPE_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS_TYPE where src_sys_nm = 'JDE-NPP')
        {% endif %} 
        and src_sys_nm = 'JDE-NPP'
    {% endif %}
)

select * from delete_captured


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_GL_TRANS_TYPE_ck not in (select distinct OPCO_GL_TRANS_TYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS_TYPE where src_sys_nm = 'JDE-NPP')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_GL_TRANS_TYPE_ck in (select distinct OPCO_GL_TRANS_TYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS_TYPE where src_sys_nm = 'JDE-NPP') and delete_dtm is not null)
    {% endif %}
{% endif %}  