

with ax_opco_item_cvrg_grp as (
    select 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(reqgroupid) as src_item_cvrg_grp_cd,
    dataareaid as opco_id,
    name as src_item_cvrg_grp_desc
    from FIVETRAN.AX_PRODREPLICATION1_DBO.REQGROUP
    where dataareaid not in ('044', '045', '047', '999')
),
final as (
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_item_cvrg_grp_cd as 
    varchar
), '') as 
    varchar
)) as opco_item_cvrg_grp_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_item_cvrg_grp_cd as 
    varchar
), '') || '-' || coalesce(cast(src_item_cvrg_grp_desc as 
    varchar
), '') as 
    varchar
)) as opco_ITEM_CVRG_GRP_ck,
            * 
    from ax_opco_item_cvrg_grp 
)

select * from final

    


    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_cvrg_grp ) 
    
