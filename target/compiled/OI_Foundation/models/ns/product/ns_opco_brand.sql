

with ns_opco_brand as(
    select 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    upper(class_id)::varchar(20) as src_brand_cd,
    name::varchar(100) as src_brand_nm,
    case 
        when isinactive = 'Yes' then 0 
        else 1 
    end::numeric(1,0) as actv_ind,
    
    
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    IFNULL(NULLIF(src_brand_nm, ''), 'NULL'), 
                                '[ ]+', ' '),
                            '[\]\[\\\/!@#$%\^*(){}|?~`,\n\t\r]', ' '), 
                        '&', ' and '), 
                    '<', '.LT.'), 
                '>', '.GT.'), 
            '[\+]', ' plus '), 
        '=', ' equal '))
    
::varchar(100) as brand_wo_spcl_chr_cd
    from FIVETRAN.NETSUITE.CLASSES
),
final as (
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_brand_cd as 
    varchar
), '') as 
    varchar
)) as opco_brand_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_brand_cd as 
    varchar
), '') || '-' || coalesce(cast(src_brand_nm as 
    varchar
), '') || '-' || coalesce(cast(brand_wo_spcl_chr_cd as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') as 
    varchar
)) as opco_brand_ck,
            *
    from ns_opco_brand 
)

select *  from final 




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_brand ) 
    
