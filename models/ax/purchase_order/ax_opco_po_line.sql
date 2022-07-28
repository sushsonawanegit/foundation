{% set _load = load_type('AX_OPCO_PO_LINE') %}

with ax_opco_po_line as(
    select 
    current_timestamp as crt_dtm,
    pl._fivetran_synced as stg_load_dtm,
    case
        when pl._fivetran_deleted = true then pl._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    pl.dataareaid as opco_id,
    pl.purchid as po_id,
    pl.linenum as po_line_nbr,
    op.opco_po_sk,
    ov.opco_vendor_sk, 
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    oip.opco_purpose_sk,
    ol.opco_lob_sk,
    oa.opco_assctn_sk,
    oi.opco_item_sk,
    oc.opco_currency_sk as trans_currency_sk,
    ots.opco_trans_status_sk,
    opt.opco_po_type_sk,
    ou.opco_uom_sk as purchase_uom_sk,
    ocoa.opco_chart_of_accts_sk as opco_gl_acct_sk,
    oirt.opco_invtry_ref_type_sk,
    opc.opco_project_catgry_sk,
    opr.opco_project_sk,
    pl.inventtransid as invtry_trans_id,
    pl.inventrefid as invtry_ref_id,
    pl.inventreftransid as invtry_ref_trans_id,
    pl.projtransid as project_item_trans_id,
    upper(pl.reqplanidsched) as req_plan_schedule_id,
    pl.reqpoid as req_plan_ordr_id,
    pl.assetid as asset_id,
    pl.projtaxgroupid as project_tax_grp_id,
    pl.name as po_line_nm,
    pl.blocked as po_line_locked_ind,
    pl.complete as po_line_dlvry_complete_ind,
    pl.scrap as scrap_ind,
    pl.taxable_opi as taxable_ind,
    pl.taxitemgroup as tax_item_grp_cd,
    pl.deliverydate as rqst_dlvry_dt,
    pl.confirmeddlv as actl_dlvry_dt,
    case    
        when pl.deliverytype = 0 then null
        when pl.deliverytype = 1 then 'Direct Delivery'
    end as dlvry_type_txt,
    pl.createddatetime as po_line_crt_dtm,
    pl.createdby as po_line_crt_by,
    upper(pl.vendgroup) as vendor_grp_cd,
    pl.externalitemid as vendor_item_id,
    pl.qtyordered as invtry_unit_ordr_qty,
    pl.purchqty as purchase_unit_ordr_qty,
    pl.remainpurchfinancial as purchase_unit_financial_remaining_qty,
    pl.remainpurchphysical as purchase_unit_physical_remaining_qty,
    pl.remaininventphysical as invtry_unit_physical_remaining_qty,
    pl.remaininventfinancial as invtry_unit_financial_remaining_qty,
    pl.purchreceivednow as purchase_unit_received_qty,
    pl.inventreceivednow as invtry_unit_received_qty,
    pl.priceunit as per_unit_qty,
    pl.linepercent as po_line_dscnt_pct,
    pl.linedisc as per_unit_line_dscnt_amt,
    pl.multilndisc as per_unit_multi_line_dscnt_amt,
    pl.purchprice as unit_purchase_price_amt,
    pl.lineamount as po_line_amt,
    pl.purchmarkup as purchase_markup_amt,
    pl.tax1099amount as tax_1099_amt
    from {{ source('AX_DEV', 'PURCHLINE') }} pl 
    left join {{ ref('opco_po')}} op 
        on pl.dataareaid = op.opco_id
        and pl.purchid = op.po_id
        and op.src_sys_nm = 'AX'
    left join {{ ref('opco_vendor')}} ov
        on upper(pl.vendaccount) = ov.vendor_id
        and ov.src_sys_nm = 'AX'
    left join {{ ref('opco_cost_center')}} occ 
        on upper(pl.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ ref('opco_dept')}} od 
        on upper(pl.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ ref('opco_type')}} ot 
        on upper(pl.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ ref('opco_purpose')}} oip
        on upper(pl.dimension4_) = oip.src_purpose_cd
        and oip.src_sys_nm = 'AX'
    left join {{ ref('opco_lob')}} ol
        on upper(pl.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('opco_assctn')}} oa
        on pl.dataareaid = oa.opco_id
        and pl.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    left join {{ ref('opco_item')}} oi 
        on pl.dataareaid = oi.opco_id
        and pl.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join {{ ref('opco_currency')}} oc 
        on upper(pl.currencycode) = oc.src_currency_cd
        and oc.src_sys_nm = 'AX'
    left join {{ ref('opco_trans_status')}} ots 
        on pl.purchstatus = ots.src_trans_status_cd
        and ots.src_sys_nm = 'AX'
    left join {{ ref('opco_po_type')}} opt 
        on pl.purchasetype = opt.src_po_type_cd
        and opt.src_sys_nm = 'AX'
    left join {{ ref('opco_uom')}} ou 
        on upper(pl.priceunit) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join {{ ref('opco_chart_of_accts')}} ocoa
        on pl.dataareaid = ocoa.opco_id
        and pl.ledgeraccount = ocoa.gl_acct_nbr
        and ocoa.src_sys_nm = 'AX'
    left join {{ ref('opco_invtry_ref_type')}} oirt 
        on pl.itemreftype = oirt.invtry_ref_type_cd
        and oirt.src_sys_nm = 'AX'
    left join {{ ref('opco_project_catgry')}} opc
        on pl.dataareaid = opc.opco_id
        and pl.projcategoryid = opc.catgry_cd
        and opc.src_sys_nm = 'AX'
    left join {{ ref('opco_project')}} opr
        on pl.dataareaid = opr.opco_id
        and pl.projid = opr.project_id
        and opr.src_sys_nm = 'AX'
    where pl.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select distinct 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'po_id', 'po_line_nbr', 'invtry_trans_id']) }} as opco_po_line_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'po_id', 'po_line_nbr', 'opco_po_sk', 'opco_vendor_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_assctn_sk', 'opco_item_sk', 'trans_currency_sk', 'opco_trans_status_sk', 'opco_po_type_sk', 'purchase_uom_sk', 'opco_gl_acct_sk', 'opco_invtry_ref_type_sk', 'opco_project_catgry_sk', 'opco_project_sk', 'invtry_trans_id', 'invtry_ref_id', 'invtry_ref_trans_id', 'project_item_trans_id', 'req_plan_schedule_id', 'req_plan_ordr_id', 'asset_id', 'project_tax_grp_id', 'po_line_nm', 'po_line_locked_ind', 'po_line_dlvry_complete_ind', 'scrap_ind', 'taxable_ind', 'tax_item_grp_cd', 'rqst_dlvry_dt', 'actl_dlvry_dt', 'dlvry_type_txt', 'po_line_crt_dtm', 'po_line_crt_by', 'vendor_grp_cd', 'vendor_item_id', 'invtry_unit_ordr_qty', 'purchase_unit_ordr_qty', 'purchase_unit_financial_remaining_qty', 'purchase_unit_physical_remaining_qty', 'invtry_unit_physical_remaining_qty', 'invtry_unit_financial_remaining_qty', 'purchase_unit_received_qty', 'invtry_unit_received_qty', 'per_unit_qty', 'po_line_dscnt_pct', 'per_unit_line_dscnt_amt', 'per_unit_multi_line_dscnt_amt', 'unit_purchase_price_amt', 'po_line_amt', 'purchase_markup_amt', 'tax_1099_amt']) }} as opco_po_line_ck,
            concat_ws('|', src_sys_nm, opco_id, po_id, po_line_nbr, invtry_trans_id) as opco_po_line_ak, 
            * 
    from ax_opco_po_line 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_PO_LINE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PO_LINE') and _load != 3%}
    union
    select
    opco_po_line_sk, opco_po_line_ck ,opco_po_line_ak, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, opco_id, po_id, po_line_nbr, opco_po_sk, opco_vendor_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk,
    opco_assctn_sk, opco_item_sk, trans_currency_sk, opco_trans_status_sk, opco_po_type_sk, purchase_uom_sk, opco_gl_acct_sk, opco_invtry_ref_type_sk,
    opco_project_catgry_sk, opco_project_sk, invtry_trans_id, invtry_ref_id, invtry_ref_trans_id, project_item_trans_id, req_plan_schedule_id, req_plan_ordr_id,
    asset_id, project_tax_grp_id, po_line_nm, po_line_locked_ind, po_line_dlvry_complete_ind, scrap_ind,taxable_ind, tax_item_grp_cd, rqst_dlvry_dt,
    actl_dlvry_dt, dlvry_type_txt, po_line_crt_dtm, po_line_crt_by, vendor_grp_cd, vendor_item_id, invtry_unit_ordr_qty, purchase_unit_ordr_qty, 
    purchase_unit_financial_remaining_qty, purchase_unit_physical_remaining_qty, invtry_unit_physical_remaining_qty, invtry_unit_financial_remaining_qty,
    purchase_unit_received_qty, invtry_unit_received_qty, per_unit_qty, po_line_dscnt_pct, per_unit_line_dscnt_amt, per_unit_multi_line_dscnt_amt,
    unit_purchase_price_amt, po_line_amt, purchase_markup_amt, tax_1099_amt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PO_LINE
    {% if _load == 1 %}
        where opco_po_ck not in (select distinct opco_po_ck from final)
    {% else %}
        where opco_po_ck not in (select distinct opco_po_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PO_LINE where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_po_ck not in (select distinct opco_po_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PO_LINE where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_po_ck in (select distinct opco_po_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PO_LINE where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  