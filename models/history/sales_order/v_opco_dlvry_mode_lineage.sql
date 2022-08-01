with v_opco_dlvry_mode_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_dlvry_mode')])}}
)

select * from v_opco_dlvry_mode_lineage