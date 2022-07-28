with opco_project_item_trans as(
    select *,
    rank() over(partition by opco_project_item_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_project_item_trans_lineage') }}
),
final as(
    select 
    opco_project_item_trans_sk, opco_project_item_trans_ck, opco_project_item_trans_ak, crt_dtm, STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, project_item_trans_id, invtry_trans_id, opco_project_sk, opco_sk, opco_item_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_project_catgry_sk, trans_uom_sk, opco_assctn_sk, opco_project_trans_status_sk, opco_project_trans_origin_sk, trans_dt, trans_desc, line_property_id, tax_grp_id, tax_item_grp_id, packing_slip_voucher_nbr, cust_invoice_nbr, project_adjsmt_ref_id, ref_project_item_trans_id, calc_ind, project_cost_trans_conversion_ind, trans_qty, per_unit_sls_price_amt, item_price_amt, total_revenue_amt, total_cost_amt, total_std_cost_amt, margin_contribution_pct


    from opco_project_item_trans 
    where rnk = 1 and delete_dtm is null
)

select * from final