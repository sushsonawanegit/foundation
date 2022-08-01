

with ns_opco_pymnt_terms as(
    select
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    payment_terms_id::varchar(40) as src_pymnt_terms_cd,
    name::varchar(100) as src_pymnt_terms_desc
    from FIVETRAN.NETSUITE.PAYMENT_TERMS
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_pymnt_terms_cd as 
    varchar
), '') as 
    varchar
)) as opco_pymnt_terms_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_pymnt_terms_cd as 
    varchar
), '') || '-' || coalesce(cast(src_pymnt_terms_desc as 
    varchar
), '') as 
    varchar
)) as opco_pymnt_terms_ck, *
    from ns_opco_pymnt_terms   
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_pymnt_terms ) 
    
