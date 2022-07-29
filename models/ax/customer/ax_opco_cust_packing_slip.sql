{% set _load = load_type('AX_OPCO_CUST_PACKING_SLIP') %}

with ax_opco_cust_packing_slip as(
    select 
    current_timestamp as crt_dtm,
    cpj._fivetran_synced as stg_load_dtm,
    case 
        when cpj._fivetran_deleted = true then cpj._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    cpj.dataareaid as opco_id,
    cpj.packingslipid as packing_slip_id,
    oso.opco_sls_ordr_sk,
    osot.opco_sls_ordr_type_sk,
    oc.opco_cust_sk,
    oc1.opco_cust_sk as invoice_cust_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    opl.opco_picking_list_sk,
    os.opco_site_sk,
    ocr.opco_currency_sk as trans_currency_sk,
    opt.opco_pymnt_terms_sk,
    wr.warehouse_sk,
    odt.opco_dlvry_terms_sk,
    odm.opco_dlvry_mode_sk,
    oln.opco_locn_sk as dlvry_locn_sk,
    cpj.ledgervoucher as gl_voucher_nbr,
    cpj.purchaseorder as po_form_nbr,
    cpj.deliverydate as ship_dt,
    cpj.customerref as cust_ref_txt,
    cpj.taxgroup_opi as tax_grp_id,
    case
        when cpj.donotprintinvoice_opi = 0 then 1
        when cpj.donotprintinvoice_opi = 1 then 0
    end as print_invoice_ind,
    cpj.creditreasonid_opi as credit_reason_id,
    cpj.specialinstructions_opi as spcl_instr_txt,
    cpj.registered_opi as registered_Ind,
    cpj.deliveryname as dlvry_cust_nm,
    cpj.invoicingname as invoice_cust_nm,
    cpj.invoicingaddress as invoice_address_txt,
    cpj.loadno_opi as load_nbr,
    cpj.plloadcount_opi as pl_load_cnt,
    cpj.miles_opi as miles_cnt,
    cpj.qty as dlvry_qty,
    cpj.palletqty_opi as pallet_qty,
    cpj.rateperpallet_opi as pallet_rate_amt,
    cpj.volume as dlvry_volume_amt,
    cpj.weight as dlvry_weight_amt,
    cpj.deliverycost_opi as dlvry_cost_amt,
    cpj.packedamount_opi as packing_slip_amt,
    cpj.invoiceid_opi as invoice_nbr,
    cpj.invoicedate_opi as invoice_dt,
    cpj.invoiced_opi as invoice_sent_ind,
    cpj.invoiceamount_opi as invoice_amt
    from {{ source('AX_DEV', 'CUSTPACKINGSLIPJOUR') }} cpj
    left join {{ ref('v_ax_opco_sls_ordr_curr')}} oso 
        on cpj.dataareaid = oso.opco_id
        and cpj.salesid = oso. sls_ordr_id
        and oso.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_sls_ordr_type_curr')}} osot 
        on cpj.salestype = osot.src_sls_ordr_type_cd
        and osot.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_cust_curr')}} oc 
        on cpj.dataareaid = oc.opco_id
        and cpj.orderaccount = oc.cust_id
        and oc.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_cust_curr')}} oc1 
        on cpj.dataareaid = oc1.opco_id
        and cpj.invoiceaccount = oc1.cust_id
        and oc.src_sys_nm = 'AX'
    left join {{ref('v_ax_opco_cost_center_curr')}} occ 
        on upper(cpj.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ref('v_ax_opco_dept_curr')}} od 
        on upper(cpj.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ref('v_ax_opco_type_curr')}} ot 
        on upper(cpj.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ref('v_ax_opco_purpose_curr')}} op 
        on upper(cpj.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join {{ref('v_ax_opco_lob_curr')}} ol
        on upper(cpj.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ref('v_ax_opco_picking_list_curr')}} opl 
        on cpj.dataareaid = opl.opco_id
        and cpj.pickinglistid_opi = opl.picking_list_id
        and opl.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_site_curr')}} os 
        on upper(cpj.inventsiteid_opi) = os.src_site_id
        and os.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_currency_curr')}} ocr
        on upper(cpj.currencycode_opi) = ocr.src_currency_cd
        and ocr.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_pymnt_terms_curr')}} opt 
        on upper(cpj.payment_opi) = opt.src_pymnt_terms_cd
        and opt.src_sys_nm = 'AX'
    left join {{ ref('v_ax_warehouse_curr')}} wr 
        on cpj.dataareaid = wr.opco_id
        and upper(cpj.inventlocationid) = wr.warehouse_id
        and wr.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_dlvry_terms_curr')}} odt 
        on upper(cpj.dlvterm) = odt.src_dlvry_terms_cd
        and odt.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_dlvry_mode_curr')}} odm 
        on upper(cpj.dlvmode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    left join {{ ref('v_ax_opco_locn_curr')}} oln 
         on upper(cpj.deliverystreet) = oln.addr_ln_1_txt
        and upper(cpj.deliverycity) = oln.city_nm
        and upper(cpj.dlvstate) = oln.state_nm
        and upper(cpj.dlvcountryregionid) = oln.country_nm
        and upper(cpj.dlvzipcode) = oln.zip_cd
        and oln.src_sys_nm = 'AX'
    where cpj.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as(
    select {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'packing_slip_id']) }} as opco_cust_packing_slip_sk,
           {{ dbt_utils.surrogate_key(['opco_id', 'packing_slip_id', 'opco_sls_ordr_sk', 'opco_sls_ordr_type_sk', 'opco_cust_sk', 'invoice_cust_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_picking_list_sk', 'opco_site_sk', 'trans_currency_sk', 'opco_pymnt_terms_sk', 'warehouse_sk', 'opco_dlvry_terms_sk', 'opco_dlvry_mode_sk', 'dlvry_locn_sk', 'gl_voucher_nbr', 'po_form_nbr', 'ship_dt', 'cust_ref_txt', 'tax_grp_id', 'print_invoice_ind', 'credit_reason_id', 'spcl_instr_txt', 'registered_Ind', 'dlvry_cust_nm', 'invoice_cust_nm', 'invoice_address_txt', 'load_nbr', 'pl_load_cnt', 'miles_cnt', 'dlvry_qty', 'pallet_qty', 'pallet_rate_amt', 'dlvry_volume_amt', 'dlvry_weight_amt', 'dlvry_cost_amt', 'packing_slip_amt', 'invoice_nbr', 'invoice_dt', 'invoice_sent_ind', 'invoice_amt']) }} as opco_cust_packing_slip_ck,
            concat_ws('|', src_sys_nm, opco_id, packing_slip_id) as opco_cust_packing_slip_ak,
            * 
    from ax_opco_cust_packing_slip ocps
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_CUST_PACKING_SLIP') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CUST_PACKING_SLIP') and _load != 3%}
    union
    select
    opco_cust_packing_slip_sk, opco_cust_packing_slip_ck, opco_cust_packing_slip_ak, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    opco_id, packing_slip_id, opco_sls_ordr_sk, opco_sls_ordr_type_sk, opco_cust_sk, invoice_cust_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, 
    opco_lob_sk, opco_picking_list_sk, opco_site_sk, trans_currency_sk, opco_pymnt_terms_sk, warehouse_sk, opco_dlvry_terms_sk, opco_dlvry_mode_sk, dlvry_locn_sk, gl_voucher_nbr, 
    po_form_nbr, ship_dt, cust_ref_txt, tax_grp_id, print_invoice_ind, credit_reason_id, spcl_instr_txt, registered_Ind, dlvry_cust_nm, invoice_cust_nm, invoice_address_txt, 
    load_nbr, pl_load_cnt, miles_cnt, dlvry_qty, pallet_qty, pallet_rate_amt, dlvry_volume_amt, dlvry_weight_amt, dlvry_cost_amt, packing_slip_amt, invoice_nbr, invoice_dt,
    invoice_sent_ind, invoice_amt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_packing_slip
    {% if _load == 1 %}
        where opco_cust_packing_slip_CK not in (select distinct opco_cust_packing_slip_CK from final)
    {% else %}
        where opco_cust_packing_slip_CK not in (select distinct opco_cust_packing_slip_CK from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_packing_slip where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_packing_slip_CK not in (select distinct opco_cust_packing_slip_CK from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_packing_slip where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_packing_slip_CK in (select distinct opco_cust_packing_slip_CK from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust_packing_slip where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  