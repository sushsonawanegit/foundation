

with ax_opco_cust_grp as(
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(custgroup) as src_cust_grp_cd,
    max(name) as src_cust_grp_desc
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTGROUP
    where dataareaid not in ('044', '045', '047', '999')
    group by src_sys_nm, src_cust_grp_cd, _fivetran_deleted
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_cust_grp_cd as 
    varchar
), '') as 
    varchar
)) as opco_cust_grp_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_cust_grp_cd as 
    varchar
), '') || '-' || coalesce(cast(src_cust_grp_desc as 
    varchar
), '') as 
    varchar
)) as opco_cust_grp_ck, 
            *
    from ax_opco_cust_grp
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_grp ) 
    
