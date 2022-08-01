with v_opco_picking_list_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_picking_list')])}}
)

select * from v_opco_picking_list_lineage