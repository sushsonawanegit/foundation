{% set _load = load_type('AX_OPCO_ASSCTN') %}

with ax_opco_assctn as (
    select 
    current_timestamp as crt_dtm,
    id._fivetran_synced as stg_load_dtm,
    case
        when id._fivetran_deleted = true then id._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    id.inventdimid as src_assctn_cd,
    id.dataareaid as opco_id,
    opco.opco_sk,
    site.opco_site_sk,
    cc.opco_cost_center_sk as cost_center_sk,
    wr.warehouse_sk,
    1 as actv_ind
    from {{source('AX_DEV', 'INVENTDIM')}} id 
    left join {{ref('ax_opco_curr')}} opco
        on id.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX' 
    left join {{ref('ax_opco_site_curr')}} site
        on upper(id.inventsiteid) = site.src_site_id
        and site.src_sys_nm = 'AX'
    left join {{ref('ax_opco_cost_center_curr')}} cc
        on upper(id.inventsiteid) = cc.src_cost_center_cd
        and cc.src_sys_nm = 'AX'
    left join {{ref('ax_warehouse_curr')}} wr 
        on upper(id.inventlocationid) = wr.warehouse_id
        and id.dataareaid = wr.opco_id
        and wr.src_sys_nm = 'AX'
    where id.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm','src_assctn_cd','opco_id']) }} as opco_assctn_sk,
    {{ dbt_utils.surrogate_key(['src_sys_nm','src_assctn_cd', 'opco_id','opco_sk','opco_site_sk','cost_center_sk','warehouse_sk','actv_ind']) }} as opco_assctn_ck, 
    * 
    from ax_opco_assctn 
)


select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_ASSCTN') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_ASSCTN') and _load != 3%}
    union
    select
    opco_assctn_sk, opco_assctn_ck, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, src_assctn_cd, opco_id, opco_sk, opco_site_sk, cost_center_sk, warehouse_sk, actv_ind  
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ASSCTN
    {% if _load == 1 %}
        where opco_assctn_ck not in (select distinct opco_assctn_ck from final)
    {% else %}
        where opco_assctn_ck not in (select distinct opco_assctn_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ASSCTN where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}

{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_assctn_ck not in (select distinct opco_assctn_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ASSCTN where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_assctn_ck in (select distinct opco_assctn_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ASSCTN where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}