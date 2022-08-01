

with ns_opco_currency as(
    select 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    currency_id::varchar(10) as src_currency_cd,
    name::varchar(50) as src_currency_nm
    from FIVETRAN.NETSUITE.CURRENCIES
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_currency_cd as 
    varchar
), '') as 
    varchar
)) as opco_currency_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_currency_cd as 
    varchar
), '') || '-' || coalesce(cast(src_currency_nm as 
    varchar
), '') as 
    varchar
)) as opco_currency_ck,
            *
    from ns_opco_currency
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_currency ) 
    
