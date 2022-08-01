

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_invtry_issue_status  as
      (with opco_invtry_issue_status as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_invtry_issue_status_lineage
)

select * from opco_invtry_issue_status
      );
    