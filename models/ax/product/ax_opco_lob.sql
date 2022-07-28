{% set _load = load_type('AX_OPCO_LOB') %}

with ax_opco_lob as(
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(num) as src_lob_cd,
    max(upper(description)) as src_lob_nm,
    {{ spcl_chr_rp('src_lob_cd')}} as lob_wo_spcl_chr_cd
    from {{source('AX_DEV', 'DIMENSIONS')}}
    where dimensioncode = 4
    and dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_lob_cd, _fivetran_deleted
),
actv_ind_grouping as(
    select upper(num) as src_lob_cd,
    avg(closed) as actv_ind
    from {{source('AX_DEV', 'DIMENSIONS')}}
    where dimensioncode = 4 
    and dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_lob_cd
),
pre_final as (
    select {{ dbt_utils.surrogate_key(['ol.src_sys_nm', 'ol.src_lob_cd']) }} as opco_lob_sk, ol.*, iff(aig.actv_ind=1, 0, 1) as actv_ind 
    from ax_opco_lob ol
    inner join actv_ind_grouping aig 
        using(src_lob_cd)  
),
final as (
    select  opco_lob_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_lob_cd', 'src_lob_nm', 'lob_wo_spcl_chr_cd', 'actv_ind']) }} as opco_lob_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_lob_cd, src_lob_nm, lob_wo_spcl_chr_cd, actv_ind
    from pre_final
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_LOB') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_LOB') and _load != 3%}
    union
    select
    OPCO_LOB_SK, OPCO_LOB_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, src_lob_cd, src_lob_nm, lob_wo_spcl_chr_cd, actv_ind
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOB
    {% if _load == 1 %}
        where OPCO_LOB_ck not in (select distinct OPCO_LOB_ck from final)
    {% else %}
        where OPCO_LOB_ck not in (select distinct OPCO_LOB_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOB where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_LOB_ck not in (select distinct OPCO_LOB_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOB where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_LOB_ck in (select distinct OPCO_LOB_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOB where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  