
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_ax_opco_sls_ordr_doc_status_curr 
  
   as (
    with v_ax_opco_sls_ordr_doc_status as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr_doc_status
)

select * from v_ax_opco_sls_ordr_doc_status
  );
