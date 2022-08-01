

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_project_catgry  as
      (with opco_project_catgry as(
    select *,
    rank() over(partition by opco_project_catgry_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_project_catgry_lineage
),
final as(
    select 
    opco_project_catgry_sk, opco_project_catgry_ck, CRT_DTM, STG_LOAD_DTM, DELETE_DTM, src_sys_nm, opco_id, catgry_cd, opco_sk, opco_item_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_uom_sk, catgry_nm, catgry_grp_cd, catgry_type_txt, empl_applicable_ind, actv_ind

    from opco_project_catgry
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    