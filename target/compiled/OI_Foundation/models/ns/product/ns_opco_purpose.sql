

with ns_opco_purpose as(
    select 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    upper(market_opi_id)::varchar(20) as src_purpose_cd,
    market_opi_name::varchar(100) as src_purpose_desc,
    case 
        when is_inactive = 'T' then 0 
        else 1 
    end::numeric(1,0) as actv_ind,
    
    
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    IFNULL(NULLIF(src_purpose_desc, ''), 'NULL'), 
                                '[ ]+', ' '),
                            '[\]\[\\\/!@#$%\^*(){}|?~`,\n\t\r]', ' '), 
                        '&', ' and '), 
                    '<', '.LT.'), 
                '>', '.GT.'), 
            '[\+]', ' plus '), 
        '=', ' equal '))
    
::varchar(100) as purpose_wo_spcl_chr_cd
    from FIVETRAN.NETSUITE.MARKET_OPI
),
final as (
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_purpose_cd as 
    varchar
), '') as 
    varchar
)) as opco_purpose_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_purpose_cd as 
    varchar
), '') || '-' || coalesce(cast(src_purpose_desc as 
    varchar
), '') || '-' || coalesce(cast(purpose_wo_spcl_chr_cd as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') as 
    varchar
)) as opco_purpose_ck,
            * 
    from ns_opco_purpose 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_purpose ) 
    
