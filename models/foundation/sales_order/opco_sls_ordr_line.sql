with opco_sls_ordr_line as(
    select *,
    rank() over(partition by opco_sls_ordr_line_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_sls_ordr_line_lineage') }}
),
final as(
    select 
    opco_sls_ordr_line_sk, opco_sls_ordr_line_ck, opco_sls_ordr_line_ak CRT_DTM, STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, sls_ordr_id, sls_ordr_line_nbr, invtry_trans_id, opco_sk, opco_sls_ordr_sk, opco_item_sk, opco_cust_sk, trans_currency_sk,
    opco_uom_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_assctn_sk, opco_trans_status_sk, opco_sls_ordr_type_sk,
    opco_dlvry_mode_sk, line_blocked_ind, line_dlvry_complete_ind, item_nm, dlvry_type_txt, po_form_nbr, drawing_locn_txt, item_bom_id, dlvry_date_contrl_txt, 
    sls_ordr_line_crt_dtm, rqst_rcpt_dt, cnfrm_rcpt_dt, rqst_ship_dt, cnfrm_ship_dt, cnfrm_dlvry_dt, grp_nm, trans_ref_id, mrkt_cd, sls_ordr_dtl_id, phase_id, 
    priority_cd, risk_ind, spcl_item_ind, spcl_in_spcl_item_ind, spcl_item_id, titan_job_nbr, tax_grp_id, tax_item_grp_txt, sls_grp_txt, sales_unit_cd,
    item_height_nbr, item_net_weight_amt, invtry_qty, ordr_qty, non_dlvrd_qty, non_invoiced_dlvrd_qty, sls_qty, sis_qty, unit_cost_price_amt, 
    unit_sls_price_amt, item_default_price_amt, item_online_price_amt, item_list_price_amt, item_cost_amt, total_sls_amt, fixed_misc_amt, imbedded_freight_amt,
    imbedded_freight_cost_amt, overhead_cost_pct, line_dscnt_pct, engineering_hours_nbr, labor_hours_nbr


    from opco_sls_ordr_line
    where rnk = 1 and delete_dtm is null
)

select * from final

