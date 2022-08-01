with opco_handling_status as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_handling_status_lineage
)

select * from opco_handling_status