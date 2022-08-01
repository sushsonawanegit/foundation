

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_gl_trans_posting_type  as
      (with opco_gl_trans_posting_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_gl_trans_posting_type_lineage
)

select * from opco_gl_trans_posting_type
      );
    