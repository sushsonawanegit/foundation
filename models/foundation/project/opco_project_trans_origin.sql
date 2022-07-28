with opco_project_trans_origin as(
    select *
    from {{ ref('v_opco_project_trans_origin_lineage') }}

)

select * from opco_project_trans_origin