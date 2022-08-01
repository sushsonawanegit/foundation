with opco_sls_ordr_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_sls_ordr_type_lineage

)

select * from opco_sls_ordr_type