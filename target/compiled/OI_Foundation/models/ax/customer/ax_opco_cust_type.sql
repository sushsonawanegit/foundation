

with ax_opco_cust_type as(
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(category_opi) as src_cust_type_cd,
    max(description_opi) as src_cust_type_desc
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTPRECASTCATEGORY_OPI
    where dataareaid not in ('044', '045', '047', '999')
    group by src_sys_nm, src_cust_type_cd, _fivetran_deleted
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_cust_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_cust_type_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_cust_type_cd as 
    varchar
), '') || '-' || coalesce(cast(src_cust_type_desc as 
    varchar
), '') as 
    varchar
)) as opco_cust_type_ck,  
            *
    from ax_opco_cust_type 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_type ) 
    
