with ax_opco_project_trans_origin as(
    select *
    from {{ ref('ax_opco_project_trans_origin') }}

)

select * from ax_opco_project_trans_origin