

with  __dbt__cte__v_ax_opco_item_type_curr as (
with v_ax_opco_item_type as(
    select *,
    rank() over(partition by opco_item_type_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_type
),
final as(
    select 
    *
    from v_ax_opco_item_type
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_item_subtype_curr as (
with v_ax_opco_item_subtype as(
    select *,
    rank() over(partition by opco_item_subtype_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_subtype
),
final as(
    select 
    *
    from v_ax_opco_item_subtype
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_uom_curr as (
with v_ax_opco_uom as(
    select *,
    rank() over(partition by opco_uom_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_uom
),
final as(
    select 
    *
    from v_ax_opco_uom
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_commsn_grp_curr as (
with v_ax_opco_commsn_grp as(
    select *,
    rank() over(partition by opco_commsn_grp_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_commsn_grp
),
final as(
    select 
    *
    from v_ax_opco_commsn_grp
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_item_grp_curr as (
with v_ax_opco_item_grp as(
    select *,
    rank() over(partition by opco_item_grp_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_grp
),
final as(
    select 
    *
    from v_ax_opco_item_grp
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_item_model_grp_curr as (
with v_ax_opco_item_model_grp as(
    select *,
    rank() over(partition by opco_item_model_grp_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_model_grp
),
final as(
    select 
    *
    from v_ax_opco_item_model_grp
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_vendor_curr as (
with v_ax_opco_vendor as(
    select *,
    rank() over(partition by opco_vendor_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor
),
final as(
    select 
    *
    from v_ax_opco_vendor
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_item_class_curr as (
with v_ax_opco_item_class as(
    select *,
    rank() over(partition by opco_item_class_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_class
),
final as(
    select 
    *
    from v_ax_opco_item_class
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_item_cvrg_grp_curr as (
with v_ax_opco_item_cvrg_grp as(
    select *,
    rank() over(partition by opco_item_cvrg_grp_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_cvrg_grp
),
final as(
    select 
    *
    from v_ax_opco_item_cvrg_grp
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_item_sls_class_curr as (
with v_ax_opco_item_sls_class as(
    select *,
    rank() over(partition by opco_item_sls_class_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_sls_class
),
final as(
    select 
    *
    from v_ax_opco_item_sls_class
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_purpose_curr as (
with v_ax_opco_purpose as(
    select *,
    rank() over(partition by opco_purpose_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_purpose
),
final as(
    select 
    *
    from v_ax_opco_purpose
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_lob_curr as (
with v_ax_opco_lob as(
    select *,
    rank() over(partition by opco_lob_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_lob
),
final as(
    select 
    *
    from v_ax_opco_lob
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_item as (
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
    from FIVETRAN.AX_PRODREPLICATION1_DBO.INVENTTABLE it 
    left join __dbt__cte__v_ax_opco_item_type_curr oit 
        on upper(it.precastcategory_opi) = oit.src_item_type_cd
        and oit.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_item_subtype_curr ois 
        on it.precastcode_opi = ois.src_item_subtype_desc
        and upper(it.precastcategory_opi) = ois.src_item_type_cd
        and ois.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_uom_curr ou 
        on upper(it.bomunitid) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_commsn_grp_curr ocg 
        on upper(it.commissiongroupid) = ocg.src_commsn_grp_cd
        and ocg.src_sys_nm ='AX'
    left join __dbt__cte__v_ax_opco_item_grp_curr oig 
        on upper(it.itemgroupid) = oig.src_item_grp_cd
        and it.dataareaid = oig.opco_id
        and oig.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_item_model_grp_curr oim 
        on upper(it.modelgroupid) = oim.src_item_model_grp_cd
        and oim.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_vendor_curr ov 
        on upper(it.primaryvendorid) = ov.vendor_id
        and ov.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_item_class_curr oic 
        on upper(it.productclassid_opi) = oic.src_item_class_cd
        and oic.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_item_cvrg_grp_curr oicg 
        on upper(it.reqgroupid) = oicg.src_item_cvrg_grp_cd
        and it.dataareaid = oicg.opco_id
        and oicg.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_item_sls_class_curr oisc 
        on upper(it.salesclassid_opi) = oisc.src_item_sls_class_cd
        and oisc.src_sys_nm = 'AX' 
    left join __dbt__cte__v_ax_opco_purpose_curr oip 
        on upper(it.dimension4_) = oip.src_purpose_cd
        and oip.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol 
        on upper(it.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    where it.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select distinct 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_item_cd as 
    varchar
), '') as 
    varchar
)) as opco_item_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_item_cd as 
    varchar
), '') || '-' || coalesce(cast(src_item_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_item_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_subtype_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_commsn_grp_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_dim_grp_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_buyer_grp_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_grp_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_model_grp_sk as 
    varchar
), '') || '-' || coalesce(cast(primary_vendor_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_class_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_cvrg_grp_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_sls_class_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_purpose_sk as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') || '-' || coalesce(cast(height_nbr as 
    varchar
), '') || '-' || coalesce(cast(width_nbr as 
    varchar
), '') || '-' || coalesce(cast(depth_nbr as 
    varchar
), '') || '-' || coalesce(cast(net_wt_nbr as 
    varchar
), '') || '-' || coalesce(cast(cost_model_ind as 
    varchar
), '') || '-' || coalesce(cast(cost_group_txt as 
    varchar
), '') || '-' || coalesce(cast(src_crt_dtm as 
    varchar
), '') || '-' || coalesce(cast(src_crt_by as 
    varchar
), '') || '-' || coalesce(cast(src_updt_dtm as 
    varchar
), '') || '-' || coalesce(cast(src_updt_by as 
    varchar
), '') || '-' || coalesce(cast(opco_lob_sk as 
    varchar
), '') || '-' || coalesce(cast(explicit_freight_ind as 
    varchar
), '') || '-' || coalesce(cast(form_id_txt as 
    varchar
), '') || '-' || coalesce(cast(form_grp_txt as 
    varchar
), '') || '-' || coalesce(cast(item_mold_txt as 
    varchar
), '') || '-' || coalesce(cast(item_admnstrn_type_desc as 
    varchar
), '') || '-' || coalesce(cast(kits_ind as 
    varchar
), '') || '-' || coalesce(cast(item_catgry_txt as 
    varchar
), '') || '-' || coalesce(cast(phantom_item_ind as 
    varchar
), '') || '-' || coalesce(cast(purchase_price_updt_ind as 
    varchar
), '') || '-' || coalesce(cast(shipping_class_txt as 
    varchar
), '') || '-' || coalesce(cast(sku_family_txt as 
    varchar
), '') || '-' || coalesce(cast(item_subcatgry_txt as 
    varchar
), '') || '-' || coalesce(cast(carton_upc_txt as 
    varchar
), '') || '-' || coalesce(cast(item_upc_txt as 
    varchar
), '') || '-' || coalesce(cast(vitality_dt as 
    varchar
), '') || '-' || coalesce(cast(tier_txt as 
    varchar
), '') as 
    varchar
)) as opco_ITEM_ck,
            * 
    from ax_opco_item
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item ) 
    
