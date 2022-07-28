with v_region as(
    {{ union_relations(
        relations=[ref('ax_region'), ref('ns_region')])}}
)

select * from v_region