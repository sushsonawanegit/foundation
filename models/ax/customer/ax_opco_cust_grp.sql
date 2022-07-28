{% set _load = load_type('AX_OPCO_CUST_GRP') %}

with ax_opco_cust_grp as(
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(custgroup) as src_cust_grp_cd,
    max(name) as src_cust_grp_desc
    from {{source('AX_DEV', 'CUSTGROUP')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_cust_grp_cd, _fivetran_deleted
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_cust_grp_cd']) }} as opco_cust_grp_sk,
            {{dbt_utils.surrogate_key(['src_sys_nm', 'src_cust_grp_cd', 'src_cust_grp_desc'])}} as opco_cust_grp_ck, 
            *
    from ax_opco_cust_grp
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_CUST_GRP') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CUST_GRP') and _load != 3%}
    union
    select
    opco_cust_grp_sk, opco_cust_grp_ck, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, src_cust_grp_cd, src_cust_grp_desc  
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_grp
    {% if _load == 1 %}
        where opco_cust_grp_ck not in (select distinct opco_cust_grp_ck from final)
    {% else %}
        where opco_cust_grp_ck not in (select distinct opco_cust_grp_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_grp where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_grp_ck not in (select distinct opco_cust_grp_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_grp where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_grp_ck in (select distinct opco_cust_grp_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_grp where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}