

with ax_opco_lob as(
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(num) as src_lob_cd,
    max(upper(description)) as src_lob_nm,
    
    
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    IFNULL(NULLIF(src_lob_cd, ''), 'NULL'), 
                                '[ ]+', ' '),
                            '[\]\[\\\/!@#$%\^*(){}|?~`,\n\t\r]', ' '), 
                        '&', ' and '), 
                    '<', '.LT.'), 
                '>', '.GT.'), 
            '[\+]', ' plus '), 
        '=', ' equal '))
    
 as lob_wo_spcl_chr_cd
    from FIVETRAN.AX_PRODREPLICATION1_DBO.DIMENSIONS
    where dimensioncode = 4
    and dataareaid not in ('044', '045', '047', '999')
    group by src_sys_nm, src_lob_cd, _fivetran_deleted
),
actv_ind_grouping as(
    select upper(num) as src_lob_cd,
    avg(closed) as actv_ind
    from FIVETRAN.AX_PRODREPLICATION1_DBO.DIMENSIONS
    where dimensioncode = 4 
    and dataareaid not in ('044', '045', '047', '999')
    group by src_lob_cd
),
pre_final as (
    select md5(cast(coalesce(cast(ol.src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(ol.src_lob_cd as 
    varchar
), '') as 
    varchar
)) as opco_lob_sk, ol.*, iff(aig.actv_ind=1, 0, 1) as actv_ind 
    from ax_opco_lob ol
    inner join actv_ind_grouping aig 
        using(src_lob_cd)  
),
final as (
    select  opco_lob_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_lob_cd as 
    varchar
), '') || '-' || coalesce(cast(src_lob_nm as 
    varchar
), '') || '-' || coalesce(cast(lob_wo_spcl_chr_cd as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') as 
    varchar
)) as opco_lob_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_lob_cd, src_lob_nm, lob_wo_spcl_chr_cd, actv_ind
    from pre_final
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_lob ) 
    
