with v_opco_cust_packing_slip_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_cust_packing_slip')])}}
)

select * from v_opco_cust_packing_slip_lineage