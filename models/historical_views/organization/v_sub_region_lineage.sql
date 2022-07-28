with v_sub_region as(
    {{ union_relations(
        relations=[ref('ax_sub_region'), ref('ns_sub_region')])}}
)

select * from v_sub_region