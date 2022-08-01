

with ax_opco_cust_code as(
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(code_opi) as src_cust_code_cd,
    max(description_opi) as src_cust_code_desc
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTPRECASTCODE_OPI
    where dataareaid not in ('044', '045', '047', '999')
    group by src_sys_nm, src_cust_code_cd, _fivetran_deleted
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_cust_code_cd as 
    varchar
), '') as 
    varchar
)) as opco_cust_code_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_cust_code_cd as 
    varchar
), '') || '-' || coalesce(cast(src_cust_code_desc as 
    varchar
), '') as 
    varchar
)) as opco_cust_code_ck, 
            *
    from ax_opco_cust_code
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_code ) 
    
