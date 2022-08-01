with v_fscl_clndr as(
    {{ union_relations(
        relations=[ref('ax_fscl_clndr')])}}
)

select * from v_fscl_clndr