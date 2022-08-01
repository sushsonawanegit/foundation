

with ax_opco_dept as (
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    upper(num)::varchar(20) as src_dept_cd,
    max(upper(description))::varchar(100) as src_dept_desc,
    
    
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    IFNULL(NULLIF(src_dept_cd, ''), 'NULL'), 
                                '[ ]+', ' '),
                            '[\]\[\\\/!@#$%\^*(){}|?~`,\n\t\r]', ' '), 
                        '&', ' and '), 
                    '<', '.LT.'), 
                '>', '.GT.'), 
            '[\+]', ' plus '), 
        '=', ' equal '))
    
::varchar(100) as dept_wo_spcl_chr_cd
    from FIVETRAN.AX_PRODREPLICATION1_DBO.DIMENSIONS
    where dimensioncode = 1
    and dataareaid not in ('044', '045', '047', '999')
    group by src_sys_nm, src_dept_cd, _fivetran_deleted
),
actv_ind_grouping as(
    select upper(num) as src_dept_cd,
    avg(closed) as actv_ind
    from FIVETRAN.AX_PRODREPLICATION1_DBO.DIMENSIONS
    where dimensioncode = 1 
    and dataareaid not in ('044', '045', '047', '999')  
    group by src_dept_cd
),
pre_final as (
    select md5(cast(coalesce(cast(od.src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(od.src_dept_cd as 
    varchar
), '') as 
    varchar
)) as opco_dept_sk, 
           od.*, iff(aig.actv_ind=1, 0, 1)::numeric(1,0) as actv_ind 
    from ax_opco_dept od
    inner join actv_ind_grouping aig 
        using(src_dept_cd)
),
final as(
    select  opco_dept_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_dept_cd as 
    varchar
), '') || '-' || coalesce(cast(src_dept_desc as 
    varchar
), '') || '-' || coalesce(cast(dept_wo_spcl_chr_cd as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') as 
    varchar
)) as opco_dept_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_dept_cd, src_dept_desc, dept_wo_spcl_chr_cd, actv_ind
    from pre_final
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dept ) 
    
