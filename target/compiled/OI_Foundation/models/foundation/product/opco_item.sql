with opco_item as(
    select *,
    rank() over(partition by opco_item_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_item_lineage
),
final as(
    select 
    opco_item_sk, opco_item_ck, crt_dtm, stg_load_dtm, delete_dtm,         
    src_sys_nm, 
    opco_id, 
    src_item_cd, 
    src_item_nm, 
    opco_item_type_sk, 
    opco_item_subtype_sk, 
    opco_uom_sk, 
    opco_commsn_grp_sk, 
    opco_item_dim_grp_sk, 
    opco_item_buyer_grp_sk, 
    opco_item_grp_sk, 
    opco_item_model_grp_sk, 
    primary_vendor_sk, 
    opco_item_class_sk, 
    opco_item_cvrg_grp_sk, 
    opco_item_sls_class_sk, 
    opco_purpose_sk, 
    actv_ind, 
    height_nbr, 
    width_nbr, 
    depth_nbr, 
    net_wt_nbr, 
    cost_model_ind, 
    cost_group_txt, 
    src_crt_dtm, 
    src_crt_by, 
    src_updt_dtm, 
    src_updt_by, 
    opco_lob_sk, 
    explicit_freight_ind, 
    form_id_txt, 
    form_grp_txt, 
    item_mold_txt, 
    item_admnstrn_type_desc, 
    kits_ind, 
    item_catgry_txt, 
    phantom_item_ind, 
    purchase_price_updt_ind, 
    shipping_class_txt, 
    sku_family_txt, 
    item_subcatgry_txt, 
    carton_upc_txt, 
    item_upc_txt, 
    vitality_dt, 
    tier_txt
    from opco_item
    where rnk = 1 and delete_dtm is null
)

select * from final