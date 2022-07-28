{% set _load = load_type('AX_OPCO_PURPOSE') %}

with ax_opco_purpose as(
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    upper(num)::varchar(20) as src_purpose_cd,
    max(upper(description))::varchar(100) as src_purpose_desc,
    {{ spcl_chr_rp('src_purpose_cd')}}::varchar(20) as purpose_wo_spcl_chr_cd
    from {{source('AX_DEV', 'DIMENSIONS')}}
    where dimensioncode = 3 
    and dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_purpose_cd, _fivetran_deleted
),
actv_ind_grouping as(
    select upper(num) as src_purpose_cd,
    avg(closed) as actv_ind
    from {{source('AX_DEV', 'DIMENSIONS')}}
    where dimensioncode = 3 
    and dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_purpose_cd
),
pre_final as (
    select {{ dbt_utils.surrogate_key(['oip.src_sys_nm', 'oip.src_purpose_cd']) }} as opco_purpose_sk, oip.* , iff(aig.actv_ind=1, 0, 1)::numeric(1,0) as actv_ind 
    from ax_opco_purpose oip
    inner join actv_ind_grouping aig 
        using(src_purpose_cd)  
),
final as (
    select  opco_purpose_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_purpose_cd', 'src_purpose_desc', 'purpose_wo_spcl_chr_cd', 'actv_ind']) }} as opco_purpose_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_purpose_cd, src_purpose_desc, purpose_wo_spcl_chr_cd, actv_ind
    from pre_final
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_PURPOSE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PURPOSE') and _load != 3%}
    union
    select
    OPCO_PURPOSE_SK, OPCO_PURPOSE_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, src_purpose_cd, src_purpose_desc, purpose_wo_spcl_chr_cd, actv_ind
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE
    {% if _load == 1 %}
        where OPCO_PURPOSE_ck not in (select distinct OPCO_PURPOSE_ck from final)
    {% else %}
        where OPCO_PURPOSE_ck not in (select distinct OPCO_PURPOSE_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PURPOSE_ck not in (select distinct OPCO_PURPOSE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PURPOSE_ck in (select distinct OPCO_PURPOSE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  