

with jde_opco_uom as (
    select 
    current_timestamp as crt_dtm,
    null::timestamp_tz as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'JDE_HNCK'::varchar(20) as src_sys_nm,
    upper(trim(drky))::varchar(15) as src_uom_cd,
    trim(drdl01)::varchar(50) as src_uom_desc
    from MULE_STAGING.JDE_HANCOCK.JDE_PRODUCTION_PRODCTL_F0005
    where trim(drsy) = '00' and trim(drrt) = 'UM'		
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_uom_cd as 
    varchar
), '') as 
    varchar
)) as opco_uom_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_uom_cd as 
    varchar
), '') || '-' || coalesce(cast(src_uom_desc as 
    varchar
), '') as 
    varchar
)) as opco_uom_ck,
            * 
    from jde_opco_uom
),
delete_captured as (
    select * from final
    
        union
        select
        opco_uom_sk, opco_uom_ck, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        src_sys_nm, src_uom_cd, src_uom_desc
        from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_UOM
        
            where OPCO_UOM_ck not in (select distinct OPCO_UOM_ck from final)
         
        and src_sys_nm = 'JDE-HNCK'
    
)

select * from delete_captured


    
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_uom ) and OPCO_UOM_ck not in (select distinct OPCO_UOM_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_UOM where src_sys_nm = 'JDE-HNCK')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_uom ) and OPCO_UOM_ck in (select distinct OPCO_UOM_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_UOM where src_sys_nm = 'JDE-HNCK') and delete_dtm is not null)
    
