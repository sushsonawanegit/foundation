{% set _load = load_type('AX_OPCO_COST_CENTER') %}

with ax_opco_cost_center as (
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    upper(num)::varchar(20) as src_cost_center_cd,
    max(upper(description))::varchar(100) as src_cost_center_desc,
    {{ spcl_chr_rp('src_cost_center_cd')}}::varchar(100) as cost_center_wo_spcl_chr_cd
    from {{source('AX_DEV', 'DIMENSIONS')}}
    where dimensioncode = 0
    and dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_cost_center_cd, _fivetran_deleted
),
actv_ind_grouping as(
    select upper(num) as src_cost_center_cd,
    avg(closed) as actv_ind
    from {{source('AX_DEV', 'DIMENSIONS')}}
    where dimensioncode = 0 
    and dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_cost_center_cd
),
pre_final as (
    select {{ dbt_utils.surrogate_key(['occ.src_sys_nm', 'occ.src_cost_center_cd']) }} as opco_cost_center_sk, 
            occ.*, iff(aig.actv_ind=1, 0, 1)::numeric(1,0) as actv_ind, null::varchar(50) as src_cost_center_type_txt 
    from ax_opco_cost_center occ
    inner join actv_ind_grouping aig 
        using(src_cost_center_cd)
),
final as(
    select opco_cost_center_sk,
           {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_cost_center_cd', 'src_cost_center_desc', 'cost_center_wo_spcl_chr_cd', 'actv_ind', 'src_cost_center_type_txt']) }} as opco_cost_center_ck,
           crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_cost_center_cd, src_cost_center_desc, cost_center_wo_spcl_chr_cd, actv_ind, src_cost_center_type_txt
    from pre_final
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_COST_CENTER') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_COST_CENTER') and _load != 3%}
    union
    select
    opco_cost_center_sk, opco_cost_center_ck, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, src_cost_center_cd, src_cost_center_desc, cost_center_wo_spcl_chr_cd, actv_ind, src_cost_center_type_txt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER
    {% if _load == 1 %}
        where opco_cost_center_ck not in (select distinct opco_cost_center_ck from final)
    {% else %}
        where opco_cost_center_ck not in (select distinct opco_cost_center_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cost_center_ck not in (select distinct opco_cost_center_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cost_center_ck in (select distinct opco_cost_center_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_COST_CENTER where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  