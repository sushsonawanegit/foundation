with v_opco_opco_locn_xref_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_opco_locn_xref')])}}
)

select * from v_opco_opco_locn_xref_lineage