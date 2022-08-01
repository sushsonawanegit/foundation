with opco_sls_ordr_doc_status as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_sls_ordr_doc_status_lineage
)

select * from opco_sls_ordr_doc_status