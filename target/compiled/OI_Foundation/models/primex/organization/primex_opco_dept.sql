

with primex_opco_dept as(
    
    
        select
        'A' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(dept)) as src_dept_cd,
        trim(dept) as src_dept_desc,
        1::number(1,0) as actv_ind,
        
    
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    IFNULL(NULLIF(src_dept_desc, ''), 'NULL'), 
                                '[ ]+', ' '),
                            '[\]\[\\\/!@#$%\^*(){}|?~`,\n\t\r]', ' '), 
                        '&', ' and '), 
                    '<', '.LT.'), 
                '>', '.GT.'), 
            '[\+]', ' plus '), 
        '=', ' equal '))
    
 as dept_wo_spcl_chr_cd
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYA_DBO_CUSGENMASTER_
         union all 
    
        select
        'C' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(dept)) as src_dept_cd,
        trim(dept) as src_dept_desc,
        1::number(1,0) as actv_ind,
        
    
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    IFNULL(NULLIF(src_dept_desc, ''), 'NULL'), 
                                '[ ]+', ' '),
                            '[\]\[\\\/!@#$%\^*(){}|?~`,\n\t\r]', ' '), 
                        '&', ' and '), 
                    '<', '.LT.'), 
                '>', '.GT.'), 
            '[\+]', ' plus '), 
        '=', ' equal '))
    
 as dept_wo_spcl_chr_cd
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYC_DBO_CUSGENMASTER_
         union all 
    
        select
        'P' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(dept)) as src_dept_cd,
        trim(dept) as src_dept_desc,
        1::number(1,0) as actv_ind,
        
    
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    IFNULL(NULLIF(src_dept_desc, ''), 'NULL'), 
                                '[ ]+', ' '),
                            '[\]\[\\\/!@#$%\^*(){}|?~`,\n\t\r]', ' '), 
                        '&', ' and '), 
                    '<', '.LT.'), 
                '>', '.GT.'), 
            '[\+]', ' plus '), 
        '=', ' equal '))
    
 as dept_wo_spcl_chr_cd
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYP_DBO_CUSGENMASTER_
         union all 
    
        select
        'X' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(dept)) as src_dept_cd,
        trim(dept) as src_dept_desc,
        1::number(1,0) as actv_ind,
        
    
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    IFNULL(NULLIF(src_dept_desc, ''), 'NULL'), 
                                '[ ]+', ' '),
                            '[\]\[\\\/!@#$%\^*(){}|?~`,\n\t\r]', ' '), 
                        '&', ' and '), 
                    '<', '.LT.'), 
                '>', '.GT.'), 
            '[\+]', ' plus '), 
        '=', ' equal '))
    
 as dept_wo_spcl_chr_cd
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYX_DBO_CUSGENMASTER_
        
    
),
de_duplication as(
    select *
           , rank() over(partition by src_dept_cd order by stg_load_dtm, src_comp) as rnk
    from primex_opco_dept
),
final as(
    select distinct
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_dept_cd as 
    varchar
), '') as 
    varchar
)) as opco_dept_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_dept_cd as 
    varchar
), '') || '-' || coalesce(cast(src_dept_desc as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') || '-' || coalesce(cast(dept_wo_spcl_chr_cd as 
    varchar
), '') as 
    varchar
)) as opco_dept_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_dept_cd, src_dept_desc, actv_ind, dept_wo_spcl_chr_cd
    from de_duplication
    where rnk = 1
),
delete_captured as(
    select * from final
    
        union
        select
        OPCO_DEPT_SK, OPCO_DEPT_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        SRC_SYS_NM, src_dept_cd, src_dept_desc, actv_ind, dept_wo_spcl_chr_cd  
        from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_DEPT
        
            where OPCO_DEPT_ck not in (select distinct OPCO_DEPT_ck from final)
         
        and src_sys_nm = 'SYSPRO-PRMX'
    
)

select * from delete_captured



    
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_dept ) and OPCO_DEPT_ck not in (select distinct OPCO_DEPT_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_DEPT where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_dept ) and OPCO_DEPT_ck in (select distinct OPCO_DEPT_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_DEPT where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    
