

with  __dbt__cte__v_ax_opco_vendor_packing_slip_curr as (
with v_ax_opco_vendor_packing_slip as(
    select *,
    rank() over(partition by opco_vendor_packing_slip_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_packing_slip
),
final as(
    select 
    *
    from v_ax_opco_vendor_packing_slip
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_curr as (
with v_ax_opco as(
    select *,
    rank() over(partition by opco_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco
),
final as(
    select 
    *
    from v_ax_opco
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
),  __dbt__cte__v_ax_opco_po_curr as (
with v_ax_opco_po as(
    select *,
    rank() over(partition by opco_po_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po
),
final as(
    select 
    *
    from v_ax_opco_po
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
),  __dbt__cte__v_ax_opco_invtry_ref_type_curr as (
with v_ax_opco_invtry_ref_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_invtry_ref_type

)

select * from v_ax_opco_invtry_ref_type
),ax_opco_vendor_packing_slip_line as (
    select 
    current_timestamp as crt_dtm,
    vps._fivetran_synced as stg_load_dtm,
    case
        when vps._fivetran_deleted = true then vps._fivetran_synced 
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    vps.dataareaid as opco_id,
    vps.packingslipid as packing_slip_id,
    vps.internalpackingslipid as internal_packing_slip_id,
    vps.inventtransid as invtry_trans_id,
    ovps.opco_vendor_packing_slip_sk,
    opco.opco_sk,
    oi.opco_item_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    opo.opco_po_sk,
    opo1.opco_po_sk as orig_po_sk,
    oa.opco_assctn_sk,
    ou.opco_uom_sk,
    oirt.opco_invtry_ref_type_sk,
    vps.linenum as line_nbr,
    vps.deliverydate as dlvry_dt,
    vps.inventdate as invtry_trans_dt,
    vps.inventrefid as invtry_ref_id,
    vps.externalitemid as vendor_item_id,
    vps.name as item_desc,
    vps.partdelivery as partial_dlvry_ind,
    vps.destcountryregionid as dest_country_nm,
    vps.deststate as dest_state_nm,
    vps.destcounty as dest_county_nm,
    vps.priceunit as per_unit_qty,
    vps.ordered as ordr_qty,
    vps.qty as received_qty,
    vps.remain as remaining_qty,
    vps.inventqty as invtry_qty,
    vps.valuemst as opco_currency_trans_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.VENDPACKINGSLIPTRANS vps
    left join __dbt__cte__v_ax_opco_vendor_packing_slip_curr ovps
        on vps.dataareaid = ovps.opco_id
        and vps.packingslipid = ovps.packing_slip_id
        and vps.internalpackingslipid = ovps.internal_packing_slip_id
        and ovps.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_curr opco 
        on vps.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_item_curr oi 
        on vps.dataareaid = oi.opco_id
        and vps.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(vps.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(vps.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(vps.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr op 
        on upper(vps.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol 
        on upper(vps.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_po_curr opo 
        on vps.dataareaid = opo.opco_id 
        and vps.purchid = opo.po_id
        and opo.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_po_curr opo1 
        on vps.dataareaid = opo1.opco_id 
        and vps.origpurchid = opo1.po_id
        and opo1.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_assctn_curr oa
        on vps.dataareaid = oa.opco_id
        and vps.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_uom_curr ou 
        on upper(vps.purchunit) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_invtry_ref_type_curr oirt 
        on vps.inventreftype = oirt.invtry_ref_type_cd
        and oirt.src_sys_nm = 'AX'
    where vps.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(packing_slip_id as 
    varchar
), '') || '-' || coalesce(cast(internal_packing_slip_id as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_id as 
    varchar
), '') as 
    varchar
)) as opco_vendor_packing_slip_line_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(packing_slip_id as 
    varchar
), '') || '-' || coalesce(cast(internal_packing_slip_id as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_id as 
    varchar
), '') || '-' || coalesce(cast(opco_vendor_packing_slip_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
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
), '') || '-' || coalesce(cast(opco_po_sk as 
    varchar
), '') || '-' || coalesce(cast(orig_po_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_assctn_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_invtry_ref_type_sk as 
    varchar
), '') || '-' || coalesce(cast(line_nbr as 
    varchar
), '') || '-' || coalesce(cast(dlvry_dt as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_dt as 
    varchar
), '') || '-' || coalesce(cast(invtry_ref_id as 
    varchar
), '') || '-' || coalesce(cast(vendor_item_id as 
    varchar
), '') || '-' || coalesce(cast(item_desc as 
    varchar
), '') || '-' || coalesce(cast(partial_dlvry_ind as 
    varchar
), '') || '-' || coalesce(cast(dest_country_nm as 
    varchar
), '') || '-' || coalesce(cast(dest_state_nm as 
    varchar
), '') || '-' || coalesce(cast(dest_county_nm as 
    varchar
), '') || '-' || coalesce(cast(per_unit_qty as 
    varchar
), '') || '-' || coalesce(cast(ordr_qty as 
    varchar
), '') || '-' || coalesce(cast(received_qty as 
    varchar
), '') || '-' || coalesce(cast(remaining_qty as 
    varchar
), '') || '-' || coalesce(cast(invtry_qty as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_trans_amt as 
    varchar
), '') as 
    varchar
)) as opco_vendor_packing_slip_line_ck,
            concat_ws('|', src_sys_nm, opco_id, packing_slip_id, internal_packing_slip_id, invtry_trans_id) as opco_vendor_packing_slip_line_ak,
            * 
    from ax_opco_vendor_packing_slip_line 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_packing_slip_line ) 
    
