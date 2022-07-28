{% set _load = load_type('AX_OPCO_TYPE') %}

with ax_opco_type as (
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    upper(num)::varchar(20) as src_type_cd,
    max(description)::varchar(100) as src_type_desc,
    {{ spcl_chr_rp('src_type_cd')}}::varchar(20) as type_wo_spcl_chr_cd
    from {{source('AX_DEV', 'DIMENSIONS')}}
    where dimensioncode = 2
    and dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_type_cd, _fivetran_deleted
),
actv_ind_grouping as(
    select upper(num) as src_type_cd,
    avg(closed) as actv_ind
    from {{source('AX_DEV', 'DIMENSIONS')}}
    where dimensioncode = 2 
    and dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_type_cd
),
pre_final as (
    select {{ dbt_utils.surrogate_key(['ot.src_sys_nm', 'ot.src_type_cd']) }} as opco_type_sk, ot.*, iff(aig.actv_ind=1, 0, 1)::numeric(1,0) as actv_ind 
    from ax_opco_type ot
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
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_TYPE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_TYPE') and _load != 3%}
    union
    select
    opco_type_sk, opco_type_ck, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm)) from final) as delete_dtm,
    src_sys_nm, src_type_cd, src_type_desc, type_wo_spcl_chr_cd, actv_ind
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE
    {% if _load == 1 %}
        where opco_type_ck not in (select distinct opco_type_ck from final)
    {% else %}
        where opco_type_ck not in (select distinct opco_type_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_type_ck not in (select distinct opco_type_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_type_ck in (select distinct opco_type_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  