

with ax_opco_cash_dscnt_terms as(
    select
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    upper(cashdisccode)::varchar(20) as src_cash_dscnt_terms_cd,
    max(description)::varchar(100) as src_cash_dscnt_terms_desc,
    max(numofdays)::number(4) as pymnt_period_in_days_nbr,
    max(percent_)::float as dscnt_pct
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CASHDISC
    where dataareaid not in ('044', '045', '047', '999')
    group by src_sys_nm, src_cash_dscnt_terms_cd, _fivetran_deleted
),
final as(
    select 
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_cash_dscnt_terms_cd as 
    varchar
), '') as 
    varchar
)) as opco_CASH_DSCNT_TERMS_sk,
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_cash_dscnt_terms_cd as 
    varchar
), '') || '-' || coalesce(cast(src_cash_dscnt_terms_desc as 
    varchar
), '') || '-' || coalesce(cast(pymnt_period_in_days_nbr as 
    varchar
), '') || '-' || coalesce(cast(dscnt_pct as 
    varchar
), '') as 
    varchar
)) as opco_CASH_DSCNT_TERMS_ck, 
        *
    from ax_opco_CASH_DSCNT_TERMS
)

select * from final



    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cash_dscnt_terms ) 
    
