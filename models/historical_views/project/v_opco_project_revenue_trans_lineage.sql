with v_opco_project_revenue_trans_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_project_revenue_trans')])}}
)

select * from v_opco_project_revenue_trans_lineage