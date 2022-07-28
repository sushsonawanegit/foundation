{% set _load = load_type('AX_OPCO_ITEM_INVTRY') %}

with ax_opco_item_invtry as (
    select 
    oi.opco_item_sk,
    oa.opco_assctn_sk,
    current_timestamp as crt_dtm,
    isu._fivetran_synced as stg_load_dtm,
    case
        when isu._fivetran_deleted = true then isu._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    isu.availphysical as item_avbl_physical_qty,
    isu.postedqty as item_posted_qty,
    isu.postedvalue as item_posted_value_amt
    from {{source('AX_DEV', 'INVENTSUM')}} isu 
    left join {{ref('opco_item')}} oi 
        on isu.dataareaid = oi.opco_id
        and isu.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join {{ref('opco_assctn')}} oa 
        on isu.dataareaid = oa.opco_id 
        and isu.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    where isu.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select distinct 
            opco_item_sk, opco_assctn_sk, 
            {{ dbt_utils.surrogate_key(['opco_item_sk', 'opco_assctn_sk', 'src_sys_nm', 'item_avbl_physical_qty', 'item_posted_qty', 'item_posted_value_amt'])}} as opco_item_invtry_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, item_avbl_physical_qty, item_posted_qty, item_posted_value_amt
    from ax_opco_item_invtry 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_ITEM_INVTRY') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_ITEM_INVTRY') and _load != 3%}
    union
    select
    opco_item_sk, opco_assctn_sk,opco_item_invtry_ck,  crt_dtm, 
    (select MAX(stg_load_dtm) from final) as stg_load_dtm,
    (select MAX(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, item_avbl_physical_qty, item_posted_qty, item_posted_value_amt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_INVTRY
    {% if _load == 1 %}
        where OPCO_ITEM_INVTRY_ck not in (select distinct OPCO_ITEM_INVTRY_ck from final)
    {% else %}
        where OPCO_ITEM_INVTRY_ck not in (select distinct OPCO_ITEM_INVTRY_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_INVTRY where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_INVTRY_ck not in (select distinct OPCO_ITEM_INVTRY_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_INVTRY where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_INVTRY_ck in (select distinct OPCO_ITEM_INVTRY_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_INVTRY where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  