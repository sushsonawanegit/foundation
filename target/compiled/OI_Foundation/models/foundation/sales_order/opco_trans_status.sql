with opco_trans_status as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_trans_status_lineage
)

select * from opco_trans_status