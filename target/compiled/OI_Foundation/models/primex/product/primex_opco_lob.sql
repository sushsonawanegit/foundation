

with primex_opco_lob as(
    
    
        select
        'A' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(fsgroup4buname)) as src_lob_cd,
        trim(fsgroup4buname) as src_lob_nm,
        1::number(1,0) as actv_ind,
        
    
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    IFNULL(NULLIF(src_lob_nm, ''), 'NULL'), 
                                '[ ]+', ' '),
                            '[\]\[\\\/!@#$%\^*(){}|?~`,\n\t\r]', ' '), 
                        '&', ' and '), 
                    '<', '.LT.'), 
                '>', '.GT.'), 
            '[\+]', ' plus '), 
        '=', ' equal '))
    
 as lob_wo_spcl_chr_cd
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYA_DBO_CUSGENMASTER_
         union all 
    
        select
        'C' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(fsgroup4buname)) as src_lob_cd,
        trim(fsgroup4buname) as src_lob_nm,
        1::number(1,0) as actv_ind,
        
    
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    IFNULL(NULLIF(src_lob_nm, ''), 'NULL'), 
                                '[ ]+', ' '),
                            '[\]\[\\\/!@#$%\^*(){}|?~`,\n\t\r]', ' '), 
                        '&', ' and '), 
                    '<', '.LT.'), 
                '>', '.GT.'), 
            '[\+]', ' plus '), 
        '=', ' equal '))
    
 as lob_wo_spcl_chr_cd
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYC_DBO_CUSGENMASTER_
         union all 
    
        select
        'P' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(fsgroup4buname)) as src_lob_cd,
        trim(fsgroup4buname) as src_lob_nm,
        1::number(1,0) as actv_ind,
        
    
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    IFNULL(NULLIF(src_lob_nm, ''), 'NULL'), 
                                '[ ]+', ' '),
                            '[\]\[\\\/!@#$%\^*(){}|?~`,\n\t\r]', ' '), 
                        '&', ' and '), 
                    '<', '.LT.'), 
                '>', '.GT.'), 
            '[\+]', ' plus '), 
        '=', ' equal '))
    
 as lob_wo_spcl_chr_cd
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYP_DBO_CUSGENMASTER_
         union all 
    
        select
        'X' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(fsgroup4buname)) as src_lob_cd,
        trim(fsgroup4buname) as src_lob_nm,
        1::number(1,0) as actv_ind,
        
    
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    IFNULL(NULLIF(src_lob_nm, ''), 'NULL'), 
                                '[ ]+', ' '),
                            '[\]\[\\\/!@#$%\^*(){}|?~`,\n\t\r]', ' '), 
                        '&', ' and '), 
                    '<', '.LT.'), 
                '>', '.GT.'), 
            '[\+]', ' plus '), 
        '=', ' equal '))
    
 as lob_wo_spcl_chr_cd
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYX_DBO_CUSGENMASTER_
        
    
),
de_duplication as(
    select *
           , rank() over(partition by src_lob_cd order by stg_load_dtm, src_comp) as rnk
    from primex_opco_lob
),
final as(
    select distinct
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_lob_cd as 
    varchar
), '') as 
    varchar
)) as opco_lob_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_lob_cd as 
    varchar
), '') || '-' || coalesce(cast(src_lob_nm as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') || '-' || coalesce(cast(lob_wo_spcl_chr_cd as 
    varchar
), '') as 
    varchar
)) as opco_lob_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_lob_cd, src_lob_nm, actv_ind, lob_wo_spcl_chr_cd
    from de_duplication
    where rnk = 1 
),
delete_captured as(
    select * from final
    
        union
        select
        OPCO_LOB_SK, OPCO_LOB_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, src_lob_cd, src_lob_nm, actv_ind, lob_wo_spcl_chr_cd
        from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_LOB
        
            where OPCO_LOB_ck not in (select distinct OPCO_LOB_ck from final)
         
        and src_sys_nm = 'SYSPRO-PRMX'
    
)

select * from delete_captured



    
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_lob ) and OPCO_LOB_ck not in (select distinct OPCO_LOB_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_LOB where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_lob ) and OPCO_LOB_ck in (select distinct OPCO_LOB_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_LOB where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    
