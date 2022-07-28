with v_opco_assctn_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_assctn')])}}
)

select * from v_opco_assctn_lineage