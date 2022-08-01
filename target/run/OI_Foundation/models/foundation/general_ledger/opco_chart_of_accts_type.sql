

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_chart_of_accts_type  as
      (with opco_chart_of_accts_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_chart_of_accts_type_lineage
)

select * from opco_chart_of_accts_type
      );
    