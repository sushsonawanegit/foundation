with opco_cust_packing_slip_line as(
    select *,
    rank() over(partition by opco_cust_packing_slip_line_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_cust_packing_slip_line_lineage') }}
),
final as(
    select 
    opco_cust_packing_slip_line_sk, opco_cust_packing_slip_line_ck, opco_cust_packing_slip_line_ak, CRT_DTM, STG_LOAD_DTM,DELETE_DTM,
    src_sys_nm, opco_id, packing_slip_id, intry_trans_id, opco_cust_packing_slip_sk, opco_sls_ordr_sk, orig_sls_ordr_sk, opco_item_sk, opco_cost_center_sk, opco_dept_sk, 
    opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_assctn_sk, opco_invtry_ref_type_sk, sls_uom_sk, invtry_uom_sk, line_nbr, cust_item_id, item_desc, invtry_ref_id, 
    invtry_ref_trans_id, ship_dt, dlvry_county_nm, dlvry_state_nm, dlvry_country_nm, partial_dlvry_ind, dlvry_type_txt, sls_grp_txt, packing_slip_line_desc, spcl_item_ind,
    spcl_item_nbr, grp_nm, per_unit_qty, invtry_unit_qty, picked_ordr_qty, packed_ordr_qty, released_qty, remaining_qty, per_unit_sls_price_amt, opco_currency_picked_line_amt,
    opco_currency_packed_line_amt, item_weight_amt
    from opco_cust_packing_slip_line
    where rnk = 1 and delete_dtm is null
)

select * from final

