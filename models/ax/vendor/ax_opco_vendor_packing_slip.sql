{% set _load = load_type('AX_OPCO_VENDOR_PACKING_SLIP') %}

with ax_opco_vendor_packing_slip as (
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
    from {{ source('AX_DEV', 'VENDPACKINGSLIPJOUR') }} vps
    left join {{ ref('ax_opco_curr')}} opco 
        on vps.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'   
    left join {{ ref('ax_opco_vendor_curr')}} ov 
        on upper(vps.orderaccount) = ov.vendor_id
        and ov.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_vendor_curr')}} ov1 
        on upper(vps.invoiceaccount) = ov1.vendor_id
        and ov1.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_po_curr')}} opo 
        on vps.dataareaid = opo.opco_id 
        and vps.purchid = opo.po_id
        and opo.src_sys_nm = 'AX'
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
    left join {{ ref('ax_opco_dlvry_terms_curr')}} odt 
        on upper(vps.dlvterm) = odt.src_dlvry_terms_cd
        and odt.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_dlvry_mode_curr')}} odm 
        on upper(vps.dlvmode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_locn_curr')}} oln 
         on upper(vps.deliverystreet) = oln.addr_ln_1_txt
        and upper(vps.deliverycity) = oln.city_nm
        and upper(vps.dlvstate) = oln.state_nm
        and upper(vps.dlvcountryregionid) = oln.country_nm
        and upper(vps.dlvzipcode) = oln.zip_cd
        and oln.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_project_curr')}} opr 
        on vps.dataareaid = opr.opco_id
        and vps.projid_opi = opr.project_id
        and opr.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_po_type_curr')}} opt 
        on vps.purchasetype = opt.src_po_type_cd
        and opt.src_sys_nm = 'AX'
    where vps.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'packing_slip_id', 'internal_packing_slip_id']) }} as opco_vendor_packing_slip_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'packing_slip_id', 'internal_packing_slip_id', 'opco_sk', 'opco_vendor_sk', 'opco_invoice_vendor_sk', 'opco_po_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_dlvry_terms_sk', 'opco_dlvry_mode_sk', 'dlvry_locn_sk', 'opco_project_sk', 'opco_po_type_sk', 'dlvry_dt', 'dlvry_vendor_nm', 'gl_voucher_nbr', 'item_buyer_grp_id', 'purchaser_nm', 'rqstd_by_nm', 'requsitioner_nm', 'attn_info_txt', 'dlvry_qty', 'dlvry_volume_amt', 'dlvry_weight_amt']) }} as opco_vendor_packing_slip_ck,
            concat_ws('|', src_sys_nm, opco_id, packing_slip_id, internal_packing_slip_id) as opco_vendor_packing_slip_ak,
            * 
    from ax_opco_vendor_packing_slip 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_opco_vendor_packing_slip') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'opco_vendor_packing_slip') and _load != 3%}
    union
    select
    opco_vendor_packing_slip_sk, opco_vendor_packing_slip_ck, opco_vendor_packing_slip_ak, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, opco_id, packing_slip_id, internal_packing_slip_id, opco_sk, opco_vendor_sk, opco_invoice_vendor_sk, opco_po_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_dlvry_terms_sk, opco_dlvry_mode_sk, dlvry_locn_sk, opco_project_sk, opco_po_type_sk, dlvry_dt, dlvry_vendor_nm, gl_voucher_nbr, item_buyer_grp_id, purchaser_nm, rqstd_by_nm, requsitioner_nm, attn_info_txt, dlvry_qty, dlvry_volume_amt, dlvry_weight_amt
    {% if _load == 1 %}
        where opco_vendor_packing_slip_ck not in (select distinct opco_vendor_packing_slip_ck from final)
    {% else %}
        where opco_vendor_packing_slip_ck not in (select distinct opco_vendor_packing_slip_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_vendor_packing_slip where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_vendor_packing_slip_ck not in (select distinct opco_vendor_packing_slip_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_vendor_packing_slip where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_vendor_packing_slip_ck in (select distinct opco_vendor_packing_slip_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_vendor_packing_slip where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}