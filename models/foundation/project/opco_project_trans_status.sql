with opco_project_trans_status as(
    select *
    from {{ ref('v_opco_project_trans_status_lineage') }}
)

select * from opco_project_trans_status