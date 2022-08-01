with ax_opco_sls_ordr_type as(
    select *
    from {{ ref('ax_opco_sls_ordr_type') }}

)

select * from ax_opco_sls_ordr_type

