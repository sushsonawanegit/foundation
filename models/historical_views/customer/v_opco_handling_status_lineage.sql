with v_opco_handling_status_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_handling_status')])}}
)

select * from v_opco_handling_status_lineage