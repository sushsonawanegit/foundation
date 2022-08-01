{% set _load = load_type('AX_OPCO_VENDOR_PACKING_SLIP_LINE') %}

with ax_opco_vendor_packing_slip_line as (
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
    from {{ source('AX_DEV', 'VENDPACKINGSLIPTRANS') }} vps
    left join {{ ref('ax_opco_vendor_packing_slip_curr')}} ovps
        on vps.dataareaid = ovps.opco_id
        and vps.packingslipid = ovps.packing_slip_id
        and vps.internalpackingslipid = ovps.internal_packing_slip_id
        and ovps.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_curr')}} opco 
        on vps.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_item_curr')}} oi 
        on vps.dataareaid = oi.opco_id
        and vps.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_cost_center_curr')}} occ 
        on upper(vps.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_dept_curr')}} od 
        on upper(vps.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_type_curr')}} ot 
        on upper(vps.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_purpose_curr')}} op 
        on upper(vps.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_lob_curr')}} ol 
        on upper(vps.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_po_curr')}} opo 
        on vps.dataareaid = opo.opco_id 
        and vps.purchid = opo.po_id
        and opo.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_po_curr')}} opo1 
        on vps.dataareaid = opo1.opco_id 
        and vps.origpurchid = opo1.po_id
        and opo1.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_assctn_curr')}} oa
        on vps.dataareaid = oa.opco_id
        and vps.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_uom_curr')}} ou 
        on upper(vps.purchunit) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_invtry_ref_type_curr')}} oirt 
        on vps.inventreftype = oirt.invtry_ref_type_cd
        and oirt.src_sys_nm = 'AX'
    where vps.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'packing_slip_id', 'internal_packing_slip_id', 'invtry_trans_id']) }} as opco_vendor_packing_slip_line_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'packing_slip_id', 'internal_packing_slip_id', 'invtry_trans_id', 'opco_vendor_packing_slip_sk', 'opco_sk', 'opco_item_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_po_sk', 'orig_po_sk', 'opco_assctn_sk', 'opco_uom_sk', 'opco_invtry_ref_type_sk', 'line_nbr', 'dlvry_dt', 'invtry_trans_dt', 'invtry_ref_id', 'vendor_item_id', 'item_desc', 'partial_dlvry_ind', 'dest_country_nm', 'dest_state_nm', 'dest_county_nm', 'per_unit_qty', 'ordr_qty', 'received_qty', 'remaining_qty', 'invtry_qty', 'opco_currency_trans_amt']) }} as opco_vendor_packing_slip_line_ck,
            concat_ws('|', src_sys_nm, opco_id, packing_slip_id, internal_packing_slip_id, invtry_trans_id) as opco_vendor_packing_slip_line_ak,
            * 
    from ax_opco_vendor_packing_slip_line 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_VENDOR_PACKING_SLIP_LINE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_VENDOR_PACKING_SLIP_LINE') and _load != 3%}
    union
    select
    OPCO_VENDOR_PACKING_SLIP_LINE_sk, OPCO_VENDOR_PACKING_SLIP_LINE_ck, opco_vendor_packing_slip_line_ak, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, opco_id, packing_slip_id, internal_packing_slip_id, invtry_trans_id, opco_vendor_packing_slip_sk, opco_sk, opco_item_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_po_sk, orig_po_sk, opco_assctn_sk, opco_uom_sk, opco_invtry_ref_type_sk, line_nbr, dlvry_dt, invtry_trans_dt, invtry_ref_id, vendor_item_id, item_desc, partial_dlvry_ind, dest_country_nm, dest_state_nm, dest_county_nm, per_unit_qty, ordr_qty, received_qty, remaining_qty, invtry_qty, opco_currency_trans_amt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_PACKING_SLIP_LINE
    {% if _load == 1 %}
        where OPCO_VENDOR_PACKING_SLIP_LINE_ck not in (select distinct OPCO_VENDOR_PACKING_SLIP_LINE_ck from final)
    {% else %}
        where OPCO_VENDOR_PACKING_SLIP_LINE_ck not in (select distinct OPCO_VENDOR_PACKING_SLIP_LINE_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_PACKING_SLIP_LINE where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_VENDOR_PACKING_SLIP_LINE_ck not in (select distinct OPCO_VENDOR_PACKING_SLIP_LINE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_PACKING_SLIP_LINE where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_VENDOR_PACKING_SLIP_LINE_ck in (select distinct OPCO_VENDOR_PACKING_SLIP_LINE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR_PACKING_SLIP_LINE where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}