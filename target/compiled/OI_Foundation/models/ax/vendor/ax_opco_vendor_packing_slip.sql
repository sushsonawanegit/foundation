

with  __dbt__cte__v_ax_opco_curr as (
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
),  __dbt__cte__v_ax_opco_dlvry_terms_curr as (
with v_ax_opco_dlvry_terms as(
    select *,
    rank() over(partition by opco_dlvry_terms_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dlvry_terms
),
final as(
    select 
    *
    from v_ax_opco_dlvry_terms
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_dlvry_mode_curr as (
with v_ax_opco_dlvry_mode as(
    select *,
    rank() over(partition by opco_dlvry_mode_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dlvry_mode
),
final as(
    select 
    *
    from v_ax_opco_dlvry_mode
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_locn_curr as (
with v_ax_opco_locn as(
    select *,
    rank() over(partition by opco_locn_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_locn
),
final as(
    select 
    *
    from v_ax_opco_locn
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_project_curr as (
with v_ax_opco_project as(
    select *,
    rank() over(partition by opco_project_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project
),
final as(
    select 
    *
    from v_ax_opco_project
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_po_type_curr as (
with v_ax_opco_po_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po_type

)

select * from v_ax_opco_po_type
),ax_opco_vendor_packing_slip as (
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
    opco.opco_sk, 
    ov.opco_vendor_sk,
    ov1.opco_vendor_sk as opco_invoice_vendor_sk,
    opo.opco_po_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    odt.opco_dlvry_terms_sk,
    odm.opco_dlvry_mode_sk,
    oln.opco_locn_sk as dlvry_locn_sk,
    opr.opco_project_sk,
    opt.opco_po_type_sk,
    vps.deliverydate as dlvry_dt,
    vps.deliveryname as dlvry_vendor_nm,
    vps.ledgervoucher as gl_voucher_nbr,
    vps.itembuyergroupid as item_buyer_grp_id,
    vps.purchplacer_opi as purchaser_nm,
    vps.requestedby_opi as rqstd_by_nm,
    vps.requisitioner as requsitioner_nm,
    vps.reqattention as attn_info_txt,
    vps.qty as dlvry_qty,
    vps.volume as dlvry_volume_amt,
    vps.weight as dlvry_weight_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.VENDPACKINGSLIPJOUR vps
    left join __dbt__cte__v_ax_opco_curr opco 
        on vps.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'   
    left join __dbt__cte__v_ax_opco_vendor_curr ov 
        on upper(vps.orderaccount) = ov.vendor_id
        and ov.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_vendor_curr ov1 
        on upper(vps.invoiceaccount) = ov1.vendor_id
        and ov1.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_po_curr opo 
        on vps.dataareaid = opo.opco_id 
        and vps.purchid = opo.po_id
        and opo.src_sys_nm = 'AX'
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
    left join __dbt__cte__v_ax_opco_dlvry_terms_curr odt 
        on upper(vps.dlvterm) = odt.src_dlvry_terms_cd
        and odt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dlvry_mode_curr odm 
        on upper(vps.dlvmode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_locn_curr oln 
         on upper(vps.deliverystreet) = oln.addr_ln_1_txt
        and upper(vps.deliverycity) = oln.city_nm
        and upper(vps.dlvstate) = oln.state_nm
        and upper(vps.dlvcountryregionid) = oln.country_nm
        and upper(vps.dlvzipcode) = oln.zip_cd
        and oln.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_project_curr opr 
        on vps.dataareaid = opr.opco_id
        and vps.projid_opi = opr.project_id
        and opr.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_po_type_curr opt 
        on vps.purchasetype = opt.src_po_type_cd
        and opt.src_sys_nm = 'AX'
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
), '') as 
    varchar
)) as opco_vendor_packing_slip_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(packing_slip_id as 
    varchar
), '') || '-' || coalesce(cast(internal_packing_slip_id as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_vendor_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_invoice_vendor_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_po_sk as 
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
), '') || '-' || coalesce(cast(opco_dlvry_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dlvry_mode_sk as 
    varchar
), '') || '-' || coalesce(cast(dlvry_locn_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_project_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_po_type_sk as 
    varchar
), '') || '-' || coalesce(cast(dlvry_dt as 
    varchar
), '') || '-' || coalesce(cast(dlvry_vendor_nm as 
    varchar
), '') || '-' || coalesce(cast(gl_voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(item_buyer_grp_id as 
    varchar
), '') || '-' || coalesce(cast(purchaser_nm as 
    varchar
), '') || '-' || coalesce(cast(rqstd_by_nm as 
    varchar
), '') || '-' || coalesce(cast(requsitioner_nm as 
    varchar
), '') || '-' || coalesce(cast(attn_info_txt as 
    varchar
), '') || '-' || coalesce(cast(dlvry_qty as 
    varchar
), '') || '-' || coalesce(cast(dlvry_volume_amt as 
    varchar
), '') || '-' || coalesce(cast(dlvry_weight_amt as 
    varchar
), '') as 
    varchar
)) as opco_vendor_packing_slip_ck,
            concat_ws('|', src_sys_nm, opco_id, packing_slip_id, internal_packing_slip_id) as opco_vendor_packing_slip_ak,
            * 
    from ax_opco_vendor_packing_slip 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_packing_slip ) 
    
