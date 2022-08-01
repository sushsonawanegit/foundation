with opco_invtry_ref_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_invtry_ref_type_lineage

)

select * from opco_invtry_ref_type