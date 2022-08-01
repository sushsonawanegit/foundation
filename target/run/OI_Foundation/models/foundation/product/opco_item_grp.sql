

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_item_grp  as
      (with opco_item_grp as(
    select *,
    rank() over(partition by opco_item_grp_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_item_grp_lineage
),
final as(
    select 
    opco_item_grp_sk, 
    opco_item_grp_ck, 
    crt_dtm, 
    stg_load_dtm, 
    delete_dtm,
    src_sys_nm, 
    src_item_grp_cd, 
    opco_id, 
    src_item_grp_desc, 
    prodn_uom_sk, 
    sales_uom_sk, 
    opco_item_freight_sk, 
    capex_only_ind, 
    calc_category_cd, 
    item_grp_type_txt, 
    explicit_freight_ind, 
    explicit_freight_pct, 
    freight_pct, 
    ic_markup_pct, 
    imbedded_freight_ind, 
    markup_pct,
    project_only_ind, 
    project_ctgry_cd
    from opco_item_grp
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    