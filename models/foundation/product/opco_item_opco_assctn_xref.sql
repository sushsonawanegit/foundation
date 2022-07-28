with opco_item_opco_assctn_xref as(
    select *,
    rank() over(partition by opco_item_sk, opco_assctn_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_item_opco_assctn_xref_lineage') }}
),
final as(
    select 
    opco_item_sk, opco_assctn_sk, crt_dtm, stg_load_dtm, delete_dtm,
    opco_item_count_grp_sk, opco_item_scrap_type_sk, src_sys_nm, bin_locn_txt, bldg_locn_txt, cubic_yrds_amt, cubic_yrds_fixed_amt, cubic_yrds_variable_amt, fixed_wt_amt, gl_prodn_qty, gl_sls_qty, labor_hrs_nbr, 
    max_on_hand_qty, net_wt_amt, prodn_abcd_cd, prodn_flushing_principle_cd, variable_wt_amt
    from opco_item_opco_assctn_xref
    where rnk = 1 and delete_dtm is null
)

select * from final