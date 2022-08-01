

with  __dbt__cte__v_ax_opco_uom_curr as (
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
),  __dbt__cte__v_ax_opco_item_freight_curr as (
with v_ax_opco_item_freight as(
    select *,
    rank() over(partition by opco_item_freight_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_freight
),
final as(
    select 
    *    
    from v_ax_opco_item_freight
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_item_grp as (
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
    from FIVETRAN.AX_PRODREPLICATION1_DBO.INVENTITEMGROUP itg 
    left join __dbt__cte__v_ax_opco_uom_curr ou1 
        on upper(itg.glproduom_opi) = ou1.src_uom_cd
        and ou1.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_uom_curr ou2 
        on upper(itg.glsalesuom_opi) = ou2.src_uom_cd
        and ou2.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_item_freight_curr oif 
        on upper(itg.ifitemid_opi) = oif.src_item_freight_cd
        and oif.src_sys_nm = 'AX'
    where dataareaid not in ('044', '045', '047', '999')
),
final as (
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_item_grp_cd as 
    varchar
), '') as 
    varchar
)) as opco_item_grp_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_item_grp_cd as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_item_grp_desc as 
    varchar
), '') || '-' || coalesce(cast(prodn_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(sales_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_freight_sk as 
    varchar
), '') || '-' || coalesce(cast(capex_only_ind as 
    varchar
), '') || '-' || coalesce(cast(calc_category_cd as 
    varchar
), '') || '-' || coalesce(cast(item_grp_type_txt as 
    varchar
), '') || '-' || coalesce(cast(explicit_freight_ind as 
    varchar
), '') || '-' || coalesce(cast(explicit_freight_pct as 
    varchar
), '') || '-' || coalesce(cast(freight_pct as 
    varchar
), '') || '-' || coalesce(cast(ic_markup_pct as 
    varchar
), '') || '-' || coalesce(cast(imbedded_freight_ind as 
    varchar
), '') || '-' || coalesce(cast(markup_pct as 
    varchar
), '') || '-' || coalesce(cast(project_only_ind as 
    varchar
), '') || '-' || coalesce(cast(project_ctgry_cd as 
    varchar
), '') as 
    varchar
)) as OPCO_ITEM_GRP_ck,
            * 
    from ax_opco_item_grp
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_grp ) 
    
