with v_opco_locn_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_locn')])}}
)

select * from v_opco_locn_lineage