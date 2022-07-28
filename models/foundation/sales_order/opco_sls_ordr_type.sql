with opco_sls_ordr_type as(
    select *
    from {{ ref('v_opco_sls_ordr_type_lineage') }}

)

select * from opco_sls_ordr_type

