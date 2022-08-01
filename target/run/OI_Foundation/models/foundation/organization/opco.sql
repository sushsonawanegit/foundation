

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco  as
      (with opco as(
    select *,
    rank() over(partition by opco_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_lineage
),
final as(
    select 
    opco_sk, opco_ck, opco_ak, crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, opco_id, opco_nm, opco_currency_sk, region_sk, sub_region_sk, dba_nm, enterprise_ind, govt_issued_id,  duns_nbr, vat_nbr, acqstn_dt, actv_ind, inactv_dt, import_vat_nbr, website_url_txt, language_txt, conversion_dt, foreign_enty_ind, combined_fed_state_tax_filer_ind, validate_1099_on_entry_ind, planning_company_ind, sls_tax_global_id, sls_tax_local_id, product_line_txt, prnt_opco_id, collection_type_txt, dashboard_usage_id
    from opco
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    