{% set _load = load_type('AX_OPCO_CUST_PACKING_SLIP_LINE') %}

with ax_opco_cust_packing_slip_line as(
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
    from {{ source('AX_DEV', 'CUSTPACKINGSLIPTRANS') }} cpt
    left join {{ ref('v_ax_opco_cust_packing_slip_curr')}} ocps
        on cpt.dataareaid = ocps.opco_id
        and cpt.packingslipid = ocps.packing_slip_id
        and src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_sls_ordr_curr')}} oso 
        on cpt.dataareaid = oso.opco_id
        and cpt.salesid = oso.sls_ordr_id
        and oso.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_sls_ordr_curr')}} oso1 
        on cpt.dataareaid = oso1.opco_id
        and cpt.origsalesid = oso1.sls_ordr_id
        and oso1.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_item_curr')}} oi 
        on cpt.dataareaid = oi.opco_id
        and cpt.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join {{ref('v_ax_opco_cost_center_curr')}} occ 
        on upper(cpt.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ref('v_ax_opco_dept_curr')}} od 
        on upper(cpt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ref('v_ax_opco_type_curr')}} ot 
        on upper(cpt.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ref('v_ax_opco_purpose_curr')}} op 
        on upper(cpt.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join {{ref('v_ax_opco_lob_curr')}} ol
        on upper(cpt.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_assctn_curr')}} oa 
        on cpt.dataareaid = oa.opco_id
        and cpt.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_invtry_ref_type_curr')}} oirt
        on cpt.inventreftype = oirt.invtry_ref_type_cd
        and oirt.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_uom_curr')}} ou 
        on upper(cpt.salesunit) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_uom_curr')}} ou1 
        on upper(cpt.inventunit_opi) = ou1.src_uom_cd
        and ou1.src_sys_nm = 'AX'
    where cpt.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'packing_slip_id', 'intry_trans_id']) }} as opco_cust_packing_slip_line_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'packing_slip_id', 'intry_trans_id', 'opco_cust_packing_slip_sk', 'opco_sls_ordr_sk', 'orig_sls_ordr_sk', 'opco_item_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_assctn_sk', 'opco_invtry_ref_type_sk', 'sls_uom_sk', 'invtry_uom_sk', 'line_nbr', 'cust_item_id', 'item_desc', 'invtry_ref_id', 'invtry_ref_trans_id', 'ship_dt', 'dlvry_county_nm', 'dlvry_state_nm', 'dlvry_country_nm', 'partial_dlvry_ind', 'dlvry_type_txt', 'sls_grp_txt', 'packing_slip_line_desc', 'spcl_item_ind', 'spcl_item_nbr', 'grp_nm', 'per_unit_qty', 'invtry_unit_qty', 'picked_ordr_qty', 'packed_ordr_qty', 'released_qty', 'remaining_qty', 'per_unit_sls_price_amt', 'opco_currency_picked_line_amt', 'opco_currency_packed_line_amt', 'item_weight_amt']) }} as opco_cust_packing_slip_line_ck,
            concat_ws('|', src_sys_nm, opco_id, packing_slip_id, intry_trans_id) as opco_cust_packing_slip_line_ak,
            * 
    from ax_opco_cust_packing_slip_line 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_CUST_PACKING_SLIP_LINE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CUST_PACKING_SLIP_LINE') and _load != 3%}
    union
    select
    opco_cust_packing_slip_line_sk, opco_cust_packing_slip_line_ck, opco_cust_packing_slip_line_ak, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, opco_id, packing_slip_id, intry_trans_id, opco_cust_packing_slip_sk, opco_sls_ordr_sk, orig_sls_ordr_sk, opco_item_sk, opco_cost_center_sk, opco_dept_sk, 
    opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_assctn_sk, opco_invtry_ref_type_sk, sls_uom_sk, invtry_uom_sk, line_nbr, cust_item_id, item_desc, invtry_ref_id, 
    invtry_ref_trans_id, ship_dt, dlvry_county_nm, dlvry_state_nm, dlvry_country_nm, partial_dlvry_ind, dlvry_type_txt, sls_grp_txt, packing_slip_line_desc, spcl_item_ind,
    spcl_item_nbr, grp_nm, per_unit_qty, invtry_unit_qty, picked_ordr_qty, packed_ordr_qty, released_qty, remaining_qty, per_unit_sls_price_amt, opco_currency_picked_line_amt,
    opco_currency_packed_line_amt, item_weight_amt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_packing_slip_line
    {% if _load == 1 %}
        where opco_cust_packing_slip_line_ck not in (select distinct opco_cust_packing_slip_line_ck from final)
    {% else %}
        where opco_cust_packing_slip_line_ck not in (select distinct opco_cust_packing_slip_line_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_packing_slip_line where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_packing_slip_line_ck not in (select distinct opco_cust_packing_slip_line_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_packing_slip_line where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_packing_slip_line_ck in (select distinct opco_cust_packing_slip_line_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_packing_slip_line where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}