{% set _load = load_type('AX_OPCO_SITE') %}

with ax_opco_site as(
    SELECT 
    current_timestamp() as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(siteid) as src_site_id,
    max(name) as src_site_nm,
    1 as actv_ind
    from {{source('AX_DEV', 'INVENTSITE')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_site_id, _fivetran_deleted 
),
final as(
    select {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_site_id']) }} as opco_site_sk,
    {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_site_id','src_site_nm','actv_ind']) }} as opco_site_ck, * 
    from ax_opco_site
   
)


select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_SITE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_SITE') and _load != 3%}
    union
    select
    opco_site_sk, opco_site_ck, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, src_site_id, src_site_nm, actv_ind  
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SITE
    {% if _load == 1 %}
        where opco_site_ck not in (select distinct opco_site_ck from final)
    {% else %}
        where opco_site_ck not in (select distinct opco_site_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SITE where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}

{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_site_ck not in (select distinct opco_site_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SITE where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_site_ck in (select distinct opco_site_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SITE where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}