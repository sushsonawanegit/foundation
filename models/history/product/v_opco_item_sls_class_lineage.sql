with v_opco_item_sls_class_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_item_sls_class')])}}
)

select * from v_opco_item_sls_class_lineage