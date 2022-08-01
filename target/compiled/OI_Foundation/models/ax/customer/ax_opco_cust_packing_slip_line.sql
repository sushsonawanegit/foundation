

with  __dbt__cte__v_ax_opco_cust_packing_slip_curr as (
with v_ax_opco_cust_packing_slip as(
    select *,
    rank() over(partition by opco_cust_packing_slip_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_packing_slip
),
final as(
    select 
    *
    from v_ax_opco_cust_packing_slip
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_sls_ordr_curr as (
with v_ax_opco_sls_ordr as(
    select *,
    rank() over(partition by opco_sls_ordr_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr
),
final as(
    select 
    *
    from v_ax_opco_sls_ordr
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_item_curr as (
with v_ax_opco_item as(
    select *,
    rank() over(partition by opco_item_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item
),
final as(
    select 
    *
    from v_ax_opco_item
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_cost_center_curr as (
with v_ax_opco_cost_center as(
    select *,
    rank() over(partition by opco_cost_center_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cost_center
),
final as(
    select 
    *
    from v_ax_opco_cost_center
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_dept_curr as (
with v_ax_opco_dept as(
    select *,
    rank() over(partition by opco_dept_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dept
),
final as(
    select 
    *
    from v_ax_opco_dept
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_type_curr as (
with v_ax_opco_type as(
    select *,
    rank() over(partition by opco_type_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_type
),
final as(
    select 
    *
    from v_ax_opco_type
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
),  __dbt__cte__v_ax_opco_assctn_curr as (
with v_ax_opco_assctn as(
    select *,
    rank() over(partition by opco_assctn_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_assctn
),
final as(
    select 
    *
    from v_ax_opco_assctn
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_invtry_ref_type_curr as (
with v_ax_opco_invtry_ref_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_invtry_ref_type

)

select * from v_ax_opco_invtry_ref_type
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
),ax_opco_cust_packing_slip_line as(
    select 
    current_timestamp as crt_dtm,
    cpt._fivetran_synced as stg_load_dtm,
    case 
        when cpt._fivetran_deleted = true then cpt._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    cpt.dataareaid as opco_id,
    cpt.packingslipid as packing_slip_id,
    cpt.inventtransid as intry_trans_id,
    ocps.opco_cust_packing_slip_sk,
    oso.opco_sls_ordr_sk,
    oso1.opco_sls_ordr_sk as orig_sls_ordr_sk,
    oi.opco_item_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    oa.opco_assctn_sk,
    oirt.opco_invtry_ref_type_sk,
    ou.opco_uom_sk as sls_uom_sk,
    ou1.opco_uom_sk as invtry_uom_sk,
    cpt.linenum as line_nbr,
    cpt.externalitemid as cust_item_id,
    cpt.name as item_desc,
    cpt.inventrefid as invtry_ref_id,
    cpt.inventreftransid as invtry_ref_trans_id,
    cpt.deliverydate as ship_dt,
    cpt.dlvcounty as dlvry_county_nm,
    cpt.dlvstate as dlvry_state_nm,
    cpt.dlvcountryregionid as dlvry_country_nm,
    cpt.partdelivery as partial_dlvry_ind,
    case 
        when cpt.deliverytype = 0 then null
        when cpt.deliverytype = 1 then 'Direct Delivery'
    end as dlvry_type_txt,
    cpt.salesgroup as sls_grp_txt,
    cpt.lineheader as packing_slip_line_desc,
    cpt.special_opi as spcl_item_ind,
    cpt.specialnumber_opi as spcl_item_nbr,
    cpt.groupname_opi as grp_nm,
    cpt.priceunit as per_unit_qty,
    cpt.inventqty as invtry_unit_qty,
    cpt.qtyordered_opi as picked_ordr_qty,
    cpt.ordered as packed_ordr_qty,
    cpt.qty as released_qty,
    cpt.remain as remaining_qty,
    cpt.salesprice_opi as per_unit_sls_price_amt,
    cpt.lineamount_opi as opco_currency_picked_line_amt,
    cpt.valuemst as opco_currency_packed_line_amt,
    cpt.weight_opi as item_weight_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTPACKINGSLIPTRANS cpt
    left join __dbt__cte__v_ax_opco_cust_packing_slip_curr ocps
        on cpt.dataareaid = ocps.opco_id
        and cpt.packingslipid = ocps.packing_slip_id
        and src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_sls_ordr_curr oso 
        on cpt.dataareaid = oso.opco_id
        and cpt.salesid = oso.sls_ordr_id
        and oso.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_sls_ordr_curr oso1 
        on cpt.dataareaid = oso1.opco_id
        and cpt.origsalesid = oso1.sls_ordr_id
        and oso1.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_item_curr oi 
        on cpt.dataareaid = oi.opco_id
        and cpt.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(cpt.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(cpt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(cpt.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr op 
        on upper(cpt.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol
        on upper(cpt.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_assctn_curr oa 
        on cpt.dataareaid = oa.opco_id
        and cpt.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_invtry_ref_type_curr oirt
        on cpt.inventreftype = oirt.invtry_ref_type_cd
        and oirt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_uom_curr ou 
        on upper(cpt.salesunit) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_uom_curr ou1 
        on upper(cpt.inventunit_opi) = ou1.src_uom_cd
        and ou1.src_sys_nm = 'AX'
    where cpt.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(packing_slip_id as 
    varchar
), '') || '-' || coalesce(cast(intry_trans_id as 
    varchar
), '') as 
    varchar
)) as opco_cust_packing_slip_line_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(packing_slip_id as 
    varchar
), '') || '-' || coalesce(cast(intry_trans_id as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_packing_slip_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sls_ordr_sk as 
    varchar
), '') || '-' || coalesce(cast(orig_sls_ordr_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cost_center_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dept_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_purpose_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_lob_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_assctn_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_invtry_ref_type_sk as 
    varchar
), '') || '-' || coalesce(cast(sls_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(invtry_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(line_nbr as 
    varchar
), '') || '-' || coalesce(cast(cust_item_id as 
    varchar
), '') || '-' || coalesce(cast(item_desc as 
    varchar
), '') || '-' || coalesce(cast(invtry_ref_id as 
    varchar
), '') || '-' || coalesce(cast(invtry_ref_trans_id as 
    varchar
), '') || '-' || coalesce(cast(ship_dt as 
    varchar
), '') || '-' || coalesce(cast(dlvry_county_nm as 
    varchar
), '') || '-' || coalesce(cast(dlvry_state_nm as 
    varchar
), '') || '-' || coalesce(cast(dlvry_country_nm as 
    varchar
), '') || '-' || coalesce(cast(partial_dlvry_ind as 
    varchar
), '') || '-' || coalesce(cast(dlvry_type_txt as 
    varchar
), '') || '-' || coalesce(cast(sls_grp_txt as 
    varchar
), '') || '-' || coalesce(cast(packing_slip_line_desc as 
    varchar
), '') || '-' || coalesce(cast(spcl_item_ind as 
    varchar
), '') || '-' || coalesce(cast(spcl_item_nbr as 
    varchar
), '') || '-' || coalesce(cast(grp_nm as 
    varchar
), '') || '-' || coalesce(cast(per_unit_qty as 
    varchar
), '') || '-' || coalesce(cast(invtry_unit_qty as 
    varchar
), '') || '-' || coalesce(cast(picked_ordr_qty as 
    varchar
), '') || '-' || coalesce(cast(packed_ordr_qty as 
    varchar
), '') || '-' || coalesce(cast(released_qty as 
    varchar
), '') || '-' || coalesce(cast(remaining_qty as 
    varchar
), '') || '-' || coalesce(cast(per_unit_sls_price_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_picked_line_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_packed_line_amt as 
    varchar
), '') || '-' || coalesce(cast(item_weight_amt as 
    varchar
), '') as 
    varchar
)) as opco_cust_packing_slip_line_ck,
            concat_ws('|', src_sys_nm, opco_id, packing_slip_id, intry_trans_id) as opco_cust_packing_slip_line_ak,
            * 
    from ax_opco_cust_packing_slip_line 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_packing_slip_line ) 
    
