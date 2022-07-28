{% set _load = load_type('NS_OPCO_TYPE') %}

with ns_opco_type as (
    select 
    current_timestamp as crt_dtm,
    null::timestamp_tz as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    upper(substr(opi_ax_acct, 8))::varchar(20) as src_type_cd,
    max (substr (opi_ax_acct, 8))::varchar(100) as src_type_desc,
    {{ spcl_chr_rp('src_type_desc')}}::varchar(100) as type_wo_spcl_chr_cd
    from {{source('NS_DEV', 'ACCOUNTS')}}
    group by src_sys_nm, src_type_cd
),
actv_ind_grouping as(
    select upper(substr(opi_ax_acct, 8)) as src_type_cd,
    avg(case when isinactive = 'Yes' then 1 else 0 end) as actv_ind
    from {{source('NS_DEV', 'ACCOUNTS')}}
    group by src_type_cd
),
pre_final as (
    select distinct {{ dbt_utils.surrogate_key(['ot.src_sys_nm', 'ot.src_type_cd']) }} as opco_type_sk, ot.*, iff(aig.actv_ind=1, 0, 1)::numeric(1,0) as actv_ind 
    from ns_opco_type ot
    inner join actv_ind_grouping aig 
        using(src_type_cd)
),
final as (
    select  opco_type_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_type_cd', 'src_type_desc', 'type_wo_spcl_chr_cd', 'actv_ind']) }} as opco_type_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_type_cd, src_type_desc, type_wo_spcl_chr_cd, actv_ind
    from pre_final
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'NS_OPCO_TYPE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_TYPE') and _load != 3%}
    union
    select
    OPCO_TYPE_SK, OPCO_TYPE_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, src_type_cd, src_type_desc, type_wo_spcl_chr_cd, actv_ind
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE
    {% if _load == 1 %}
        where OPCO_TYPE_ck not in (select distinct OPCO_TYPE_ck from final)
    {% else %}
        where OPCO_TYPE_ck not in (select distinct OPCO_TYPE_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE where src_sys_nm = 'NS')
    {% endif %} 
    and src_sys_nm = 'NS'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_TYPE_ck not in (select distinct OPCO_TYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE where src_sys_nm = 'NS')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_TYPE_ck in (select distinct OPCO_TYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE where src_sys_nm = 'NS') and delete_dtm is not null)
    {% endif %}
{% endif %}  