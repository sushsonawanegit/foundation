{% set _load = load_type('AX_OPCO_ITEM_GRP') %}

with ax_opco_item_grp as (
    select
    current_timestamp as crt_dtm,
    itg._fivetran_synced as stg_load_dtm,
    case
        when itg._fivetran_deleted = true then itg._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(itg.itemgroupid) as src_item_grp_cd,
    itg.dataareaid as opco_id,
    itg.name as src_item_grp_desc,
    ou1.opco_uom_sk as prodn_uom_sk,
    ou2.opco_uom_sk as sales_uom_sk,
    oif.opco_item_freight_sk as opco_item_freight_sk,
    itg.capexonly_opi as capex_only_ind,
    itg.calccategory_opi as calc_category_cd,
    itg.dimension_opi3_ as item_grp_type_txt,
    itg.ef_opi as explicit_freight_ind,
    itg.efpercentage_opi as explicit_freight_pct,
    itg.freight_opi as freight_pct,
    itg.icmarkuppercent_opi as ic_markup_pct,
    itg.if_opi as imbedded_freight_ind,
    itg.ifpercentage_opi as imbedded_freight_pct,
    itg.markup_opi as markup_pct,
    itg.projectonly_opi as project_only_ind,
    itg.projcategoryid_opi as project_ctgry_cd
    from {{source('AX_DEV', 'INVENTITEMGROUP')}} itg 
    left join {{ref('opco_uom')}} ou1 
        on upper(itg.glproduom_opi) = ou1.src_uom_cd
        and ou1.src_sys_nm = 'AX'
    left join {{ref('opco_uom')}} ou2 
        on upper(itg.glsalesuom_opi) = ou2.src_uom_cd
        and ou2.src_sys_nm = 'AX'
    left join {{ref('opco_item_freight')}} oif 
        on upper(itg.ifitemid_opi) = oif.src_item_freight_cd
        and oif.src_sys_nm = 'AX'
    where dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_item_grp_cd']) }} as opco_item_grp_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_item_grp_cd', 'opco_id', 'src_item_grp_desc', 'prodn_uom_sk', 'sales_uom_sk', 'opco_item_freight_sk', 'capex_only_ind', 'calc_category_cd', 'item_grp_type_txt', 'explicit_freight_ind', 'explicit_freight_pct', 'freight_pct', 'ic_markup_pct', 'imbedded_freight_ind', 'markup_pct', 'project_only_ind', 'project_ctgry_cd']) }} as OPCO_ITEM_GRP_ck,
            * 
    from ax_opco_item_grp
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_ITEM_GRP') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_ITEM_GRP') and _load != 3%}
    union
    select
    OPCO_ITEM_GRP_SK, OPCO_ITEM_GRP_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, src_item_grp_cd, opco_id, src_item_grp_desc, prodn_uom_sk, sales_uom_sk, opco_item_freight_sk, capex_only_ind, calc_category_cd, item_grp_type_txt, explicit_freight_ind, explicit_freight_pct, freight_pct, ic_markup_pct, imbedded_freight_ind, markup_pct, project_only_ind, project_ctgry_cd
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_GRP
    {% if _load == 1 %}
        where OPCO_ITEM_GRP_ck not in (select distinct OPCO_ITEM_GRP_ck from final)
    {% else %}
        where OPCO_ITEM_GRP_ck not in (select distinct OPCO_ITEM_GRP_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_GRP where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_GRP_ck not in (select distinct OPCO_ITEM_GRP_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_GRP where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_GRP_ck in (select distinct OPCO_ITEM_GRP_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_GRP where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  