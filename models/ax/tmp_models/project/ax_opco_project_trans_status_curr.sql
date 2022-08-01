with ax_opco_project_trans_status as(
    select *
    from {{ ref('ax_opco_project_trans_status') }}
)

select * from ax_opco_project_trans_status