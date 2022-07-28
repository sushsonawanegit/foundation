with v_warehouse_lineage as(
    {{ union_relations(
        relations=[ref('ax_warehouse')])}}
)

select * from v_warehouse_lineage