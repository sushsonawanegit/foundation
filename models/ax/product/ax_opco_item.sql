{% set _load = load_type('AX_OPCO_ITEM') %}

with ax_opco_item as (
    select 
    current_timestamp as crt_dtm,
    it._fivetran_synced as stg_load_dtm,
    case
        when it._fivetran_deleted = true then it._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    it.itemid as src_item_cd,
    it.dataareaid as opco_id,
    it.itemname as src_item_nm,
    oit.opco_item_type_sk,
    ois.opco_item_subtype_sk,
    ou.opco_uom_sk,
    ocg.opco_commsn_grp_sk,
    '' as opco_item_dim_grp_sk,
    '' as opco_item_buyer_grp_sk,
    oig.opco_item_grp_sk,
    oim.opco_item_model_grp_sk,
    ov.opco_vendor_sk as primary_vendor_sk,
    oic.opco_item_class_sk,
    oicg.opco_item_cvrg_grp_sk,
    oisc.opco_item_sls_class_sk,
    oip.opco_purpose_sk,
    iff(it.stopped_opi=0, 1, 0) as actv_ind,
    it.height as height_nbr,
    it.width as width_nbr,
    it.depth as depth_nbr,
    it.netweight as net_wt_nbr,
    it.costmodel as cost_model_ind,
    it.costgroupid as cost_group_txt,
    it.createddatetime as src_crt_dtm,
    it.createdby as src_crt_by,
    it.modifieddatetime as src_updt_dtm,
    it.modifiedby as src_updt_by,
    ol.opco_lob_sk,
    it.explicitfreight_opi as explicit_freight_ind,
    it.form_opi as form_id_txt,
    it.formgroup_opi as form_grp_txt,
    it.itemmold_opi as item_mold_txt,
    case 
        when it.itemtype = 0
        then 'Item'
        when it.itemtype = 1
        then 'BOM'
        when it.itemtype = 2
        then 'Service'
        else null
    end as item_admnstrn_type_desc,
    it.kits_opi as kits_ind,
    it.pcat_opi as item_catgry_txt,
    it.phantom as phantom_item_ind,
    it.purchmodel as purchase_price_updt_ind,
    it.shippingclass_opi as shipping_class_txt,
    it.skufamily_opi as sku_family_txt,
    it.subpcat_opi as item_subcatgry_txt,
    it.upccarton_opi as carton_upc_txt,
    it.upcitem_opi as item_upc_txt,
    it.vitalitydate_opi as vitality_dt,
    case
        when tier_opi = 0 then 'None'
        when tier_opi = 1 then '1'
        when tier_opi = 2 then '2'
        when tier_opi = 3 then '3'
        when tier_opi = 4 then 'A'
        when tier_opi = 5 then 'B'
        when tier_opi = 6 then 'C'
    end as tier_txt
    from {{source('AX_DEV', 'INVENTTABLE')}} it 
    left join {{ref('ax_opco_item_type_curr')}} oit 
        on upper(it.precastcategory_opi) = oit.src_item_type_cd
        and oit.src_sys_nm = 'AX'
    left join {{ref('ax_opco_item_subtype_curr')}} ois 
        on it.precastcode_opi = ois.src_item_subtype_desc
        and upper(it.precastcategory_opi) = ois.src_item_type_cd
        and ois.src_sys_nm = 'AX'
    left join {{ref('ax_opco_uom_curr')}} ou 
        on upper(it.bomunitid) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join {{ref('ax_opco_commsn_grp_curr')}} ocg 
        on upper(it.commissiongroupid) = ocg.src_commsn_grp_cd
        and ocg.src_sys_nm ='AX'
    left join {{ref('ax_opco_item_grp_curr')}} oig 
        on upper(it.itemgroupid) = oig.src_item_grp_cd
        and it.dataareaid = oig.opco_id
        and oig.src_sys_nm = 'AX'
    left join {{ref('ax_opco_item_model_grp_curr')}} oim 
        on upper(it.modelgroupid) = oim.src_item_model_grp_cd
        and oim.src_sys_nm = 'AX'
    left join {{ref('ax_opco_vendor_curr')}} ov 
        on upper(it.primaryvendorid) = ov.vendor_id
        and ov.src_sys_nm = 'AX'
    left join {{ref('ax_opco_item_class_curr')}} oic 
        on upper(it.productclassid_opi) = oic.src_item_class_cd
        and oic.src_sys_nm = 'AX'
    left join {{ref('ax_opco_item_cvrg_grp_curr')}} oicg 
        on upper(it.reqgroupid) = oicg.src_item_cvrg_grp_cd
        and it.dataareaid = oicg.opco_id
        and oicg.src_sys_nm = 'AX'
    left join {{ref('ax_opco_item_sls_class_curr')}} oisc 
        on upper(it.salesclassid_opi) = oisc.src_item_sls_class_cd
        and oisc.src_sys_nm = 'AX' 
    left join {{ref('ax_opco_purpose_curr')}} oip 
        on upper(it.dimension4_) = oip.src_purpose_cd
        and oip.src_sys_nm = 'AX'
    left join {{ref('ax_opco_lob_curr')}} ol 
        on upper(it.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    where it.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select distinct 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_item_cd']) }} as opco_item_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_item_cd', 'src_item_nm', 'opco_item_type_sk', 'opco_item_subtype_sk', 'opco_uom_sk', 'opco_commsn_grp_sk', 'opco_item_dim_grp_sk', 'opco_item_buyer_grp_sk', 'opco_item_grp_sk', 'opco_item_model_grp_sk', 'primary_vendor_sk', 'opco_item_class_sk', 'opco_item_cvrg_grp_sk', 'opco_item_sls_class_sk', 'opco_purpose_sk', 'actv_ind', 'height_nbr', 'width_nbr', 'depth_nbr', 'net_wt_nbr', 'cost_model_ind', 'cost_group_txt', 'src_crt_dtm', 'src_crt_by', 'src_updt_dtm', 'src_updt_by', 'opco_lob_sk', 'explicit_freight_ind', 'form_id_txt', 'form_grp_txt', 'item_mold_txt', 'item_admnstrn_type_desc', 'kits_ind', 'item_catgry_txt', 'phantom_item_ind', 'purchase_price_updt_ind', 'shipping_class_txt', 'sku_family_txt', 'item_subcatgry_txt', 'carton_upc_txt', 'item_upc_txt', 'vitality_dt', 'tier_txt']) }} as opco_ITEM_ck,
            * 
    from ax_opco_item
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_ITEM') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_ITEM') and _load != 3%}
    union
    select
    OPCO_ITEM_SK, OPCO_ITEM_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, opco_id, src_item_cd, src_item_nm, opco_item_type_sk, opco_item_subtype_sk, opco_uom_sk, opco_commsn_grp_sk, opco_item_dim_grp_sk, opco_item_buyer_grp_sk, opco_item_grp_sk, opco_item_model_grp_sk, primary_vendor_sk, opco_item_class_sk, opco_item_cvrg_grp_sk, opco_item_sls_class_sk, opco_purpose_sk, actv_ind, height_nbr, width_nbr, depth_nbr, net_wt_nbr, cost_model_ind, cost_group_txt, src_crt_dtm, src_crt_by, src_updt_dtm, src_updt_by, opco_lob_sk, explicit_freight_ind, form_id_txt, form_grp_txt, item_mold_txt, item_admnstrn_type_desc, kits_ind, item_catgry_txt, phantom_item_ind, purchase_price_updt_ind, shipping_class_txt, sku_family_txt, item_subcatgry_txt, carton_upc_txt, item_upc_txt, vitality_dt, tier_txt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM
    {% if _load == 1 %}
        where OPCO_ITEM_ck not in (select distinct OPCO_ITEM_ck from final)
    {% else %}
        where OPCO_ITEM_ck not in (select distinct OPCO_ITEM_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_ck not in (select distinct OPCO_ITEM_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_ck in (select distinct OPCO_ITEM_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}