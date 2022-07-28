{% set _load = load_type('AX_OPCO_DEPT') %}

with ax_opco_dept as (
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    upper(num)::varchar(20) as src_dept_cd,
    max(upper(description))::varchar(100) as src_dept_desc,
    {{ spcl_chr_rp('src_dept_cd')}}::varchar(100) as dept_wo_spcl_chr_cd
    from {{source('AX_DEV', 'DIMENSIONS')}}
    where dimensioncode = 1
    and dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_dept_cd, _fivetran_deleted
),
actv_ind_grouping as(
    select upper(num) as src_dept_cd,
    avg(closed) as actv_ind
    from {{source('AX_DEV', 'DIMENSIONS')}}
    where dimensioncode = 1 
    and dataareaid not in {{ var('excluded_ax_companies')}}  
    group by src_dept_cd
),
pre_final as (
    select {{ dbt_utils.surrogate_key(['od.src_sys_nm', 'od.src_dept_cd']) }} as opco_dept_sk, 
           od.*, iff(aig.actv_ind=1, 0, 1)::numeric(1,0) as actv_ind 
    from ax_opco_dept od
    inner join actv_ind_grouping aig 
        using(src_dept_cd)
),
final as(
    select  opco_dept_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_dept_cd', 'src_dept_desc', 'dept_wo_spcl_chr_cd', 'actv_ind']) }} as opco_dept_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_dept_cd, src_dept_desc, dept_wo_spcl_chr_cd, actv_ind
    from pre_final
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_DEPT') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_DEPT') and _load != 3%}
    union
    select
    opco_dept_sk, opco_dept_ck, crt_dtm,   
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, src_dept_cd, src_dept_desc, dept_wo_spcl_chr_cd, actv_ind
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_DEPT
    {% if _load == 1 %}
        where opco_dept_ck not in (select distinct opco_dept_ck from final)
    {% else %}
        where opco_dept_ck not in (select distinct opco_dept_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_DEPT where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_dept_ck not in (select distinct opco_dept_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_DEPT where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_dept_ck in (select distinct opco_dept_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_DEPT where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  