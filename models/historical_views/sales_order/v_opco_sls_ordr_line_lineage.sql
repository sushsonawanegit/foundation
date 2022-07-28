with v_opco_sls_ordr_line_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_sls_ordr_line')])}}
)

select * from v_opco_sls_ordr_line_lineage