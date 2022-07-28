with v_opco_site_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_site')])}}
)

select * from v_opco_site_lineage