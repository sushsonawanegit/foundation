

with ax_opco_cust as(
    select 
    concat_ws('|', 'AX', accountnum, dataareaid) as opco_cust_ak,
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,    
    'AX' as src_sys_nm,
    accountnum as cust_id,
    name as cust_nm,
    dataareaid as opco_id,
    namealias as dba_nm,
    '' as govt_issued_id,
    '' as duns_nbr,
    '' as lob_nm,
    vatnum as vat_number,
    '' as language_txt,
    countryregionid as country_cd,
    url as website_url_txt,
    taxexemptnumber_opi as tax_exempt_nbr,
    taxgroup as tax_grp_txt,
    custitemgroupid as cust_item_grp_cd,
    dimension4_ as end_market_nm,
    collectorcode_opi as collector_cd,
    partyid as party_id,
    flag_opi as flag_id
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTTABLE 
    where dataareaid not in ('044', '045', '047', '999')
),
final as(
    select 
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(cust_id as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') as 
    varchar
)) as opco_cust_sk,
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(cust_id as 
    varchar
), '') || '-' || coalesce(cast(cust_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(dba_nm as 
    varchar
), '') || '-' || coalesce(cast(govt_issued_id as 
    varchar
), '') || '-' || coalesce(cast(duns_nbr as 
    varchar
), '') || '-' || coalesce(cast(lob_nm as 
    varchar
), '') || '-' || coalesce(cast(vat_number as 
    varchar
), '') || '-' || coalesce(cast(language_txt as 
    varchar
), '') || '-' || coalesce(cast(country_cd as 
    varchar
), '') || '-' || coalesce(cast(website_url_txt as 
    varchar
), '') || '-' || coalesce(cast(tax_exempt_nbr as 
    varchar
), '') || '-' || coalesce(cast(tax_grp_txt as 
    varchar
), '') || '-' || coalesce(cast(cust_item_grp_cd as 
    varchar
), '') || '-' || coalesce(cast(end_market_nm as 
    varchar
), '') || '-' || coalesce(cast(collector_cd as 
    varchar
), '') || '-' || coalesce(cast(party_id as 
    varchar
), '') || '-' || coalesce(cast(flag_id as 
    varchar
), '') as 
    varchar
)) as opco_cust_ck, 
        *
    from ax_opco_cust
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust ) 
    
