

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_gl_trans_type  as
      (

with opco_gl_trans_type as(
    select 
    current_timestamp as crt_dtm,
    'NS'::varchar(20) as src_sys_nm,
    upper(transaction_type)::varchar(50) as src_gl_trans_type_cd,
    max(transaction_type)::varchar(100) as src_gl_trans_type_desc
    from FIVETRAN.NETSUITE.TRANSACTIONS
    group by src_sys_nm, src_gl_trans_type_cd
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_gl_trans_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_type_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_gl_trans_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_type_ck, 
            *
    from opco_gl_trans_type
)

select * from final
      );
    