with syspro_primex_opco as(
    select *
    from {{ ref('syspro_primex_opco') }}
)

select * from syspro_primex_opco