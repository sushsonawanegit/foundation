

with ax_opco_commsn_grp as(
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX'::varchar(15) as src_sys_nm,
    upper(groupid)::varchar(50) as src_commsn_grp_cd,
    max(name)::varchar(100) as src_commsn_grp_desc,
    'CUST'::varchar(20) as src_commsn_grp_type_txt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.COMMISSIONCUSTOMERGROUP
    where dataareaid not in ('044', '045', '047', '999')
    group by src_sys_nm, src_commsn_grp_type_txt, src_commsn_grp_cd, _fivetran_deleted
),
final as(
    select 
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_commsn_grp_cd as 
    varchar
), '') as 
    varchar
)) as opco_commsn_grp_sk,
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_commsn_grp_cd as 
    varchar
), '') || '-' || coalesce(cast(src_commsn_grp_desc as 
    varchar
), '') || '-' || coalesce(cast(src_commsn_grp_type_txt as 
    varchar
), '') as 
    varchar
)) as opco_commsn_grp_ck, 
        *
    from ax_opco_COMMSN_GRP
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_commsn_grp ) 
    
