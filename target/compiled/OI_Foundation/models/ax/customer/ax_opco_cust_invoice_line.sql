

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
),  __dbt__cte__v_ax_opco_chart_of_accts_curr as (
with v_ax_opco_chart_of_accts as(
    select *,
    rank() over(partition by opco_chart_of_accts_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_chart_of_accts
),
final as(
    select 
    *
    from v_ax_opco_chart_of_accts
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
),  __dbt__cte__v_ax_opco_currency_curr as (
with v_ax_opco_currency as(
    select *,
    rank() over(partition by opco_currency_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_currency
),
final as(
    select 
    *
    from v_ax_opco_currency
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
),ax_opco_cust_invoice_line as (
    select 
    current_timestamp as crt_dtm,
    cit._fivetran_synced as stg_load_dtm,
    case
        when cit._fivetran_deleted = true then cit._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    cit.dataareaid as opco_id,
    cit.recid as src_key_txt,
    opco.opco_sk,
    oso.opco_sls_ordr_sk,
    oso1.opco_sls_ordr_sk as orig_sls_ordr_sk,
    oi.opco_item_sk,
    ocoa.opco_chart_of_accts_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    ocr.opco_currency_sk as trans_currency_sk,
    ou.opco_uom_sk as sls_uom_sk,
    ou1.opco_uom_sk as gl_prod_uom_sk,
    ou2.opco_uom_sk as gl_sls_uom_sk,
    oa.opco_assctn_sk,
    oirt.opco_invtry_ref_type_sk,
    cit.invoiceid as invoice_nbr,
    cit.linenum as line_nbr,
    cit.inventtransid as invtry_trans_id,
    cit.externalitemid as cust_item_id,
    cit.taxgroup as tax_grp_id,
    cit.taxitemgroup as tax_item_grp_id,
    cit.inventrefid as invtry_ref_id,
    cit.itembomid_opi as item_bom_id,
    cit.name as item_desc,
    cit.salesgroup as sls_grp_txt,
    cit.countryregionofshipment as shpmt_country_nm,
    cit.dlvcountryregionid as dlvry_country_nm,
    cit.dlvstate as dlvry_state_nm,
    cit.dlvcounty as dlvry_county_nm,
    cit.mark_opi as mark_txt,
    cit.groupname_opi as grp_nm,
    cit.dlvdate as ship_dt,
    cit.partdelivery as partial_dlvry_ind,
    cit.risktype_opi as risk_type_ind,
    cit.kits_opi as kits_ind,
    cit.commisscalc as commsn_calc_ind,
    case 
        when cit.deliverytype = 0 then null
        when cit.deliverytype = 1 then 'Direct Delivery'
    end as dlvry_type_txt,
    cit.lineheader as invoice_line_txt,
    cit.special_opi as spcl_item_ind,
    cit.specialinspecial_opi as spcl_in_spcl_ind,
    cit.specialnumber_opi as spcl_item_nbr,
    cit.laborhours_opi as labor_hours_nbr,
    cit.engineeringhours_opi as engineering_hours_nbr,
    cit.weight as item_weight_amt,
    cit.priceunit as per_unit_qty,
    cit.qty as invoice_item_qty,
    cit.qtyordered_opi as ordr_qty,
    cit.qtyphysical as dlvrd_qty,
    cit.qtyprevinvoiced_opi as prev_invoiced_qty,
    cit.qtyremain_opi as remaining_qty,
    cit.inventqty as invtry_units_qty,
    cit.glprodqty_opi as gl_prod_qty,
    cit.glsalesqty_opi as gl_sls_qty,
    cit.sisqty_opi as sis_qty,
    cit.discpercent as dscnt_pct,
    cit.linepercent as line_dscnt_pct,
    cit.overheadcostpct_opi as overhead_cost_pct,
    cit.mc_opi as margin_contribution_pct,
    cit.lineamount as trans_currency_sls_amt,
    cit.lineamountmst as opco_currency_sls_amt,
    cit.lineamounttax as trans_currency_incl_sls_tax_amt,
    cit.lineamounttaxmst as opco_currency_incl_sls_tax_amt,
    cit.sumlinedisc as trans_currency_sum_line_dscnt_amt,
    cit.sumlinediscmst as opco_currency_sum_line_dscnt_amt,
    cit.taxamount as trans_currency_tax_amt,
    cit.taxamountmst as opco_currency_tax_amt,
    cit.commissamountcur as trans_currency_commsn_amt,
    cit.commissamountmst as opco_currency_commsn_amt,
    cit.salesmarkup as sls_markup_amt,
    cit.salesprice as per_unit_sls_price_amt,
    cit.ediprice_opi as online_price_amt,
    cit.listprice_opi as list_price_amt,
    cit.imbfrtcost_opi as imbedded_freight_cost_amt,
    cit.imbfrt_opi as imbedded_freight_amt,
    cit.totalcost_opi as total_cost_amt,
    cit.itemcost_opi as item_cost_amt,
    cit.defaultprice_opi as default_item_price_amt,
    cit.itemprice_opi as item_price_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTINVOICETRANS cit 
    left join __dbt__cte__v_ax_opco_curr opco 
        on cit.dataareaid = opco.opco_id 
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_sls_ordr_curr oso 
        on cit.dataareaid = oso.opco_id
        and cit.salesid = oso.sls_ordr_id
        and oso.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_sls_ordr_curr oso1 
        on cit.dataareaid = oso1.opco_id
        and cit.origsalesid = oso1.sls_ordr_id
        and oso1.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_item_curr oi 
        on cit.dataareaid = oi.opco_id
        and cit.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_chart_of_accts_curr ocoa 
        on cit.dataareaid = ocoa.opco_id
        and cit.ledgeraccount = ocoa.gl_acct_nbr
        and ocoa.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(cit.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(cit.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(cit.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr op 
        on upper(cit.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol
        on upper(cit.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr ocr 
        on upper(cit.currencycode) = ocr.src_currency_cd
        and ocr.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_uom_curr ou 
        on upper(cit.salesunit) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_uom_curr ou1 
        on upper(cit.glproduom_opi) = ou1.src_uom_cd
        and ou1.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_uom_curr ou2 
        on upper(cit.glsalesuom_opi) = ou2.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_assctn_curr oa 
        on cit.dataareaid = oa.opco_id
        and cit.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_invtry_ref_type_curr oirt
        on cit.inventreftype = oirt.invtry_ref_type_cd
        and oirt.src_sys_nm = 'AX'
    where cit.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') as 
    varchar
)) as opco_cust_invoice_line_sk,
           md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sls_ordr_sk as 
    varchar
), '') || '-' || coalesce(cast(orig_sls_ordr_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_chart_of_accts_sk as 
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
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(sls_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(gl_prod_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(gl_sls_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_assctn_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_invtry_ref_type_sk as 
    varchar
), '') || '-' || coalesce(cast(invoice_nbr as 
    varchar
), '') || '-' || coalesce(cast(line_nbr as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_id as 
    varchar
), '') || '-' || coalesce(cast(cust_item_id as 
    varchar
), '') || '-' || coalesce(cast(tax_grp_id as 
    varchar
), '') || '-' || coalesce(cast(tax_item_grp_id as 
    varchar
), '') || '-' || coalesce(cast(invtry_ref_id as 
    varchar
), '') || '-' || coalesce(cast(item_bom_id as 
    varchar
), '') || '-' || coalesce(cast(item_desc as 
    varchar
), '') || '-' || coalesce(cast(sls_grp_txt as 
    varchar
), '') || '-' || coalesce(cast(shpmt_country_nm as 
    varchar
), '') || '-' || coalesce(cast(dlvry_country_nm as 
    varchar
), '') || '-' || coalesce(cast(dlvry_state_nm as 
    varchar
), '') || '-' || coalesce(cast(dlvry_county_nm as 
    varchar
), '') || '-' || coalesce(cast(mark_txt as 
    varchar
), '') || '-' || coalesce(cast(grp_nm as 
    varchar
), '') || '-' || coalesce(cast(ship_dt as 
    varchar
), '') || '-' || coalesce(cast(partial_dlvry_ind as 
    varchar
), '') || '-' || coalesce(cast(risk_type_ind as 
    varchar
), '') || '-' || coalesce(cast(kits_ind as 
    varchar
), '') || '-' || coalesce(cast(commsn_calc_ind as 
    varchar
), '') || '-' || coalesce(cast(dlvry_type_txt as 
    varchar
), '') || '-' || coalesce(cast(invoice_line_txt as 
    varchar
), '') || '-' || coalesce(cast(spcl_item_ind as 
    varchar
), '') || '-' || coalesce(cast(spcl_in_spcl_ind as 
    varchar
), '') || '-' || coalesce(cast(spcl_item_nbr as 
    varchar
), '') || '-' || coalesce(cast(labor_hours_nbr as 
    varchar
), '') || '-' || coalesce(cast(engineering_hours_nbr as 
    varchar
), '') || '-' || coalesce(cast(item_weight_amt as 
    varchar
), '') || '-' || coalesce(cast(per_unit_qty as 
    varchar
), '') || '-' || coalesce(cast(invoice_item_qty as 
    varchar
), '') || '-' || coalesce(cast(ordr_qty as 
    varchar
), '') || '-' || coalesce(cast(dlvrd_qty as 
    varchar
), '') || '-' || coalesce(cast(prev_invoiced_qty as 
    varchar
), '') || '-' || coalesce(cast(remaining_qty as 
    varchar
), '') || '-' || coalesce(cast(invtry_units_qty as 
    varchar
), '') || '-' || coalesce(cast(gl_prod_qty as 
    varchar
), '') || '-' || coalesce(cast(gl_sls_qty as 
    varchar
), '') || '-' || coalesce(cast(sis_qty as 
    varchar
), '') || '-' || coalesce(cast(dscnt_pct as 
    varchar
), '') || '-' || coalesce(cast(line_dscnt_pct as 
    varchar
), '') || '-' || coalesce(cast(overhead_cost_pct as 
    varchar
), '') || '-' || coalesce(cast(margin_contribution_pct as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sls_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_sls_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_incl_sls_tax_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_incl_sls_tax_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sum_line_dscnt_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_sum_line_dscnt_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_tax_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_tax_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_commsn_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_commsn_amt as 
    varchar
), '') || '-' || coalesce(cast(sls_markup_amt as 
    varchar
), '') || '-' || coalesce(cast(per_unit_sls_price_amt as 
    varchar
), '') || '-' || coalesce(cast(online_price_amt as 
    varchar
), '') || '-' || coalesce(cast(list_price_amt as 
    varchar
), '') || '-' || coalesce(cast(imbedded_freight_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(imbedded_freight_amt as 
    varchar
), '') || '-' || coalesce(cast(total_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(item_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(default_item_price_amt as 
    varchar
), '') || '-' || coalesce(cast(item_price_amt as 
    varchar
), '') as 
    varchar
)) as opco_cust_invoice_line_ck,
            concat_ws('|', src_sys_nm, opco_id, src_key_txt) as opco_cust_invoice_line_ak,
            * 
    from ax_opco_cust_invoice_line 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_invoice_line ) 
    
