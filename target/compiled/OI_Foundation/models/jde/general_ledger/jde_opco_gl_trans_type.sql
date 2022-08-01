

with jde_opco_gl_trans_type as (
    select 
    current_timestamp as crt_dtm,
    null::timestamp_tz as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'JDE_HNCK'::varchar(20) as src_sys_nm,
    upper(trim(drky))::varchar(50) as src_gl_trans_type_cd,
    trim(drdl01)::varchar(100) as src_gl_trans_type_desc
    from MULE_STAGING.JDE_HANCOCK.JDE_PRODUCTION_PRODCTL_F0005
    where trim(drsy) = '00' and trim(drrt) = 'DT'		
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_gl_trans_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_type_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_gl_trans_type_cd as 
    varchar
), '') || '-' || coalesce(cast(src_gl_trans_type_desc as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_type_ck,
            * 
    from jde_opco_gl_trans_type
),
delete_captured as (
    select * from final
    
        union
        select
        OPCO_GL_TRANS_TYPE_sk, OPCO_GL_TRANS_TYPE_ck, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        src_sys_nm, src_gl_trans_type_cd, src_gl_trans_type_desc
        from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_GL_TRANS_TYPE
        
            where OPCO_GL_TRANS_TYPE_ck not in (select distinct OPCO_GL_TRANS_TYPE_ck from final)
         
        and src_sys_nm = 'JDE-HNCK'
    
)

select * from delete_captured



    
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_gl_trans_type ) and OPCO_GL_TRANS_TYPE_ck not in (select distinct OPCO_GL_TRANS_TYPE_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_GL_TRANS_TYPE where src_sys_nm = 'JDE-HNCK')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_gl_trans_type ) and OPCO_GL_TRANS_TYPE_ck in (select distinct OPCO_GL_TRANS_TYPE_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_GL_TRANS_TYPE where src_sys_nm = 'JDE-HNCK') and delete_dtm is not null)
    
