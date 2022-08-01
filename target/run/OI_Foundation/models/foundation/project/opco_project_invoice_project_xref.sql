

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_project_invoice_project_xref  as
      (with opco_project_invoice_project_xref  as(
    select *,
    rank() over(partition by opco_project_sk, invoice_project_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_project_invoice_project_xref_lineage
),
final as(
    select 
    opco_project_sk, invoice_project_sk, crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, opco_id, invoice_project_id
    from opco_project_invoice_project_xref
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    