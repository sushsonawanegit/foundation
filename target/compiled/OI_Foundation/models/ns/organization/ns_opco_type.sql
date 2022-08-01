

with ns_opco_type as (
    select 
    current_timestamp as crt_dtm,
    null::timestamp_tz as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    upper(substr(opi_ax_acct, 8))::varchar(20) as src_type_cd,
    max (substr (opi_ax_acct, 8))::varchar(100) as src_type_desc,
    
    
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    IFNULL(NULLIF(src_type_desc, ''), 'NULL'), 
                                '[ ]+', ' '),
                            '[\]\[\\\/!@#$%\^*(){}|?~`,\n\t\r]', ' '), 
                        '&', ' and '), 
                    '<', '.LT.'), 
                '>', '.GT.'), 
            '[\+]', ' plus '), 
        '=', ' equal '))
    
::varchar(100) as type_wo_spcl_chr_cd
    from FIVETRAN.NETSUITE.ACCOUNTS
    group by src_sys_nm, src_type_cd
),
actv_ind_grouping as(
    select upper(substr(opi_ax_acct, 8)) as src_type_cd,
    avg(case when isinactive = 'Yes' then 1 else 0 end) as actv_ind
    from FIVETRAN.NETSUITE.ACCOUNTS
    group by src_type_cd
),
pre_final as (
    select distinct md5(cast(coalesce(cast(ot.src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(ot.src_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_type_sk, ot.*, iff(aig.actv_ind=1, 0, 1)::numeric(1,0) as actv_ind 
    from ns_opco_type ot
    inner join actv_ind_grouping aig 
        using(src_type_cd)
),
final as (
    select  opco_type_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_type_cd as 
    varchar
), '') || '-' || coalesce(cast(src_type_desc as 
    varchar
), '') || '-' || coalesce(cast(type_wo_spcl_chr_cd as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') as 
    varchar
)) as opco_type_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_type_cd, src_type_desc, type_wo_spcl_chr_cd, actv_ind
    from pre_final
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_type ) 
    
