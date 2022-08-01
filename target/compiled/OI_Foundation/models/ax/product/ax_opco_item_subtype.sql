

with ax_opco_item_subtype as(
    select
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    code_opi as src_item_subtype_cd,
    upper(category_opi) as src_item_type_cd,
    max(description_opi) as src_item_subtype_desc
    from FIVETRAN.AX_PRODREPLICATION1_DBO.INVENTPRECASTCODE_OPI
    where dataareaid not in ('044', '045', '047', '999')
    group by src_sys_nm, src_item_type_cd, src_item_subtype_cd, _fivetran_deleted
),
final as (
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_item_type_cd as 
    varchar
), '') || '-' || coalesce(cast(src_item_subtype_cd as 
    varchar
), '') as 
    varchar
)) as OPCO_ITEM_SUBTYPE_SK,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_item_type_cd as 
    varchar
), '') || '-' || coalesce(cast(src_item_subtype_cd as 
    varchar
), '') || '-' || coalesce(cast(src_item_subtype_desc as 
    varchar
), '') as 
    varchar
)) as OPCO_ITEM_SUBTYPE_ck, 
            * 
    from ax_opco_item_subtype 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_subtype ) 
    
