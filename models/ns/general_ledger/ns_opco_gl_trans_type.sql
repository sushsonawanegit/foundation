{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_gl_trans_type as(
    select 
    current_timestamp as crt_dtm,
    'NS'::varchar(20) as src_sys_nm,
    upper(transaction_type)::varchar(50) as src_gl_trans_type_cd,
    max(transaction_type)::varchar(100) as src_gl_trans_type_desc
    from {{ source('NS_DEV', 'TRANSACTIONS')}}
    group by src_sys_nm, src_gl_trans_type_cd
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_gl_trans_type_cd']) }} as opco_gl_trans_type_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_gl_trans_type_cd']) }} as opco_gl_trans_type_ck, 
            *
    from opco_gl_trans_type
)

select * from final