

with jde_opco_cost_center as (
    select 
    current_timestamp as crt_dtm,
    null::timestamp_tz as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'JDE-HNCK'::varchar(20) as src_sys_nm,
    upper(trim(mcmcu))::varchar(20) as src_cost_center_cd,
    trim(mcdl01)::varchar(100) as src_cost_center_desc,
    '1'::number(1,0) as actv_ind,
    trim(mcstyl)::varchar(50) as src_cost_center_type_txt,
    
    
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    IFNULL(NULLIF(src_cost_center_desc, ''), 'NULL'), 
                                '[ ]+', ' '),
                            '[\]\[\\\/!@#$%\^*(){}|?~`,\n\t\r]', ' '), 
                        '&', ' and '), 
                    '<', '.LT.'), 
                '>', '.GT.'), 
            '[\+]', ' plus '), 
        '=', ' equal '))
    
::varchar(100) as cost_center_wo_spcl_chr_cd
    from MULE_STAGING.JDE_HANCOCK.JDE_PRODUCTION_PRODDTA_F0006
),
final as (
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_cost_center_cd as 
    varchar
), '') as 
    varchar
)) as opco_cost_center_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_cost_center_cd as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') || '-' || coalesce(cast(src_cost_center_type_txt as 
    varchar
), '') || '-' || coalesce(cast(cost_center_wo_spcl_chr_cd as 
    varchar
), '') as 
    varchar
)) as opco_cost_center_ck,
            *
    from jde_opco_cost_center
),
delete_captured as (
    select * from final
    
        union
        select
        OPCO_COST_CENTER_SK, OPCO_COST_CENTER_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, src_cost_center_cd, src_cost_center_desc, actv_ind, src_cost_center_type_txt, cost_center_wo_spcl_chr_cd
        from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_COST_CENTER
        
            where OPCO_COST_CENTER_ck not in (select distinct OPCO_COST_CENTER_ck from final)
         
        and src_sys_nm = 'JDE-HNCK'
    
)

select * from delete_captured



    
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_cost_center ) and OPCO_COST_CENTER_ck not in (select distinct OPCO_COST_CENTER_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_COST_CENTER where src_sys_nm = 'JDE-HNCK')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_cost_center ) and OPCO_COST_CENTER_ck in (select distinct OPCO_COST_CENTER_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_COST_CENTER where src_sys_nm = 'JDE-HNCK') and delete_dtm is not null)
    
