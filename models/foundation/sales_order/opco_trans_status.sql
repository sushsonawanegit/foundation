with opco_trans_status as(
    select *
    from {{ ref('v_opco_trans_status_lineage') }}
)

select * from opco_trans_status

