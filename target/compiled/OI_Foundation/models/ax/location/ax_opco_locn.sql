

with ax_opco_locn1 as(
    select distinct
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    '' as std_locn_sk,
    'AX' as src_sys_nm,
    upper(street) as addr_ln_1_txt,
    '' as addr_ln_2_txt,
    '' as addr_ln_3_txt,
    upper(city) as city_nm,
    upper(state) as state_nm,
    upper(countryregionid) as country_nm,
    upper(zipcode) as zip_cd,
    max(upper(county)) as county_nm
    from FIVETRAN.AX_PRODREPLICATION1_DBO.ADDRESS
    where dataareaid not in ('044', '045', '047', '999')
    group by addr_ln_1_txt, addr_ln_2_txt, addr_ln_3_txt, city_nm, state_nm, country_nm, zip_cd, _fivetran_deleted
),
ax_opco_locn2 as(
    select distinct
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    '' as std_locn_sk,
    'AX' as src_sys_nm,
    upper(street) as addr_ln_1_txt,
    '' as addr_ln_2_txt,
    '' as addr_ln_3_txt,
    upper(city) as city_nm,
    upper(state) as state_nm,
    upper(countryregionid) as country_nm,
    upper(zipcode) as zip_cd,
    max(upper(county)) as county_nm
    from FIVETRAN.AX_PRODREPLICATION1_DBO.COMPANYINFO
    where dataareaid not in ('044', '045', '047', '999')
    group by addr_ln_1_txt, addr_ln_2_txt, addr_ln_3_txt, city_nm, state_nm, country_nm, zip_cd, _fivetran_deleted
),
ax_opco_locn3 as(
    select distinct
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    '' as std_locn_sk,
    'AX' as src_sys_nm,
    upper(street) as addr_ln_1_txt,
    '' as addr_ln_2_txt,
    '' as addr_ln_3_txt,
    upper(city) as city_nm,
    upper(state) as state_nm,
    upper(countryregionid) as country_nm,
    upper(zipcode) as zip_cd,
    max(upper(county)) as county_nm
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTTABLE
    where dataareaid not in ('044', '045', '047', '999')
    group by addr_ln_1_txt, addr_ln_2_txt, addr_ln_3_txt, city_nm, state_nm, country_nm, zip_cd, _fivetran_deleted
),
ax_opco_locn4 as(
    select distinct
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    '' as std_locn_sk,
    'AX' as src_sys_nm,
    upper(deliverystreet) as addr_ln_1_txt,
    '' as addr_ln_2_txt,
    '' as addr_ln_3_txt,
    upper(deliverycity) as city_nm,
    upper(deliverystate) as state_nm,
    upper(deliverycountryregionid) as country_nm,
    upper(deliveryzipcode) as zip_cd,
    max(upper(deliverycounty)) as county_nm
    from FIVETRAN.AX_PRODREPLICATION1_DBO.SALESTABLE
    where dataareaid not in ('044', '045', '047', '999')
    group by addr_ln_1_txt, addr_ln_2_txt, addr_ln_3_txt, city_nm, state_nm, country_nm, zip_cd, _fivetran_deleted
),
ax_opco_locn5 as(
    select distinct
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    '' as std_locn_sk,
    'AX' as src_sys_nm,
    upper(street) as addr_ln_1_txt,
    '' as addr_ln_2_txt,
    '' as addr_ln_3_txt,
    upper(city) as city_nm,
    upper(state) as state_nm,
    upper(countryregionid) as country_nm,
    upper(zipcode) as zip_cd,
    max(upper(county)) as county_nm
    from FIVETRAN.AX_PRODREPLICATION1_DBO.VENDTABLE
    where dataareaid not in ('044', '045', '047', '999')
    group by addr_ln_1_txt, addr_ln_2_txt, addr_ln_3_txt, city_nm, state_nm, country_nm, zip_cd, _fivetran_deleted
),
ax_opco_locn6 as(
    select distinct
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    '' as std_locn_sk,
    'AX' as src_sys_nm,
    upper(deliverystreet) as addr_ln_1_txt,
    '' as addr_ln_2_txt,
    '' as addr_ln_3_txt,
    upper(deliverycity) as city_nm,
    upper(dlvstate) as state_nm,
    upper(dlvcountryregionid) as country_nm,
    upper(dlvzipcode) as zip_cd,
    max(upper(dlvcounty)) as county_nm
    from FIVETRAN.AX_PRODREPLICATION1_DBO.PROJTABLE
    where dataareaid not in ('044', '045', '047', '999')
    group by addr_ln_1_txt, addr_ln_2_txt, addr_ln_3_txt, city_nm, state_nm, country_nm, zip_cd, _fivetran_deleted
),
ax_opco_locn7 as(
    select distinct
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    '' as std_locn_sk,
    'AX' as src_sys_nm,
    upper(deliverystreet) as addr_ln_1_txt,
    '' as addr_ln_2_txt,
    '' as addr_ln_3_txt,
    upper(deliverycity) as city_nm,
    upper(dlvstate) as state_nm,
    upper(dlvcountryregionid) as country_nm,
    upper(dlvzipcode) as zip_cd,
    max(upper(dlvcounty)) as county_nm
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTINVOICEJOUR
    where dataareaid not in ('044', '045', '047', '999')
    group by addr_ln_1_txt, addr_ln_2_txt, addr_ln_3_txt, city_nm, state_nm, country_nm, zip_cd, _fivetran_deleted
),
ax_opco_locn8 as(
    select distinct
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    '' as std_locn_sk,
    'AX' as src_sys_nm,
    upper(invoicestreet) as addr_ln_1_txt,
    '' as addr_ln_2_txt,
    '' as addr_ln_3_txt,
    upper(invoicecity) as city_nm,
    upper(invstate) as state_nm,
    upper(invcountryregionid) as country_nm,
    upper(invzipcode) as zip_cd,
    max(upper(invcounty)) as county_nm
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTINVOICEJOUR
    where dataareaid not in ('044', '045', '047', '999')
    group by addr_ln_1_txt, addr_ln_2_txt, addr_ln_3_txt, city_nm, state_nm, country_nm, zip_cd, _fivetran_deleted
),
ax_opco_locn9 as(
    select distinct
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    '' as std_locn_sk,
    'AX' as src_sys_nm,
    upper(deliverystreet) as addr_ln_1_txt,
    '' as addr_ln_2_txt,
    '' as addr_ln_3_txt,
    upper(deliverycity) as city_nm,
    upper(dlvstate) as state_nm,
    upper(dlvcountryregionid) as country_nm,
    upper(dlvzipcode) as zip_cd,
    max(upper(dlvcounty)) as county_nm
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTPACKINGSLIPJOUR
    where dataareaid not in ('044', '045', '047', '999')
    group by addr_ln_1_txt, addr_ln_2_txt, addr_ln_3_txt, city_nm, state_nm, country_nm, zip_cd, _fivetran_deleted
),
ax_opco_locn10 as(
    select distinct
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    '' as std_locn_sk,
    'AX' as src_sys_nm,
    upper(deliverystreet) as addr_ln_1_txt,
    '' as addr_ln_2_txt,
    '' as addr_ln_3_txt,
    upper(deliverycity) as city_nm,
    upper(dlvstate) as state_nm,
    upper(dlvcountryregionid) as country_nm,
    upper(deliveryzipcode) as zip_cd,
    max(upper(dlvcounty)) as county_nm
    from FIVETRAN.AX_PRODREPLICATION1_DBO.PURCHTABLE
    where dataareaid not in ('044', '045', '047', '999')
    group by addr_ln_1_txt, addr_ln_2_txt, addr_ln_3_txt, city_nm, state_nm, country_nm, zip_cd, _fivetran_deleted
),
ax_opco_locn11 as(
    select distinct
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    '' as std_locn_sk,
    'AX' as src_sys_nm,
    upper(deliverystreet) as addr_ln_1_txt,
    '' as addr_ln_2_txt,
    '' as addr_ln_3_txt,
    upper(deliverycity) as city_nm,
    upper(dlvstate) as state_nm,
    upper(dlvcountryregionid) as country_nm,
    upper(dlvzipcode) as zip_cd,
    max(upper(dlvcounty)) as county_nm
    from FIVETRAN.AX_PRODREPLICATION1_DBO.VENDPACKINGSLIPJOUR
    where dataareaid not in ('044', '045', '047', '999')
    group by addr_ln_1_txt, addr_ln_2_txt, addr_ln_3_txt, city_nm, state_nm, country_nm, zip_cd, _fivetran_deleted
),
pre_final as (
    select * from ax_opco_locn1
    union 
    select * from ax_opco_locn2
    union
    select * from ax_opco_locn3
    union
    select * from ax_opco_locn4
    union
    select * from ax_opco_locn5
    union
    select * from ax_opco_locn6
    union
    select * from ax_opco_locn7
    union
    select * from ax_opco_locn8
    union
    select * from ax_opco_locn9
    union
    select * from ax_opco_locn10
    union
    select * from ax_opco_locn11
),
final as(
    select distinct 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(std_locn_sk as 
    varchar
), '') || '-' || coalesce(cast(addr_ln_1_txt as 
    varchar
), '') || '-' || coalesce(cast(addr_ln_2_txt as 
    varchar
), '') || '-' || coalesce(cast(addr_ln_3_txt as 
    varchar
), '') || '-' || coalesce(cast(city_nm as 
    varchar
), '') || '-' || coalesce(cast(state_nm as 
    varchar
), '') || '-' || coalesce(cast(country_nm as 
    varchar
), '') || '-' || coalesce(cast(zip_cd as 
    varchar
), '') as 
    varchar
)) as opco_locn_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(std_locn_sk as 
    varchar
), '') || '-' || coalesce(cast(addr_ln_1_txt as 
    varchar
), '') || '-' || coalesce(cast(addr_ln_2_txt as 
    varchar
), '') || '-' || coalesce(cast(addr_ln_3_txt as 
    varchar
), '') || '-' || coalesce(cast(city_nm as 
    varchar
), '') || '-' || coalesce(cast(state_nm as 
    varchar
), '') || '-' || coalesce(cast(country_nm as 
    varchar
), '') || '-' || coalesce(cast(zip_cd as 
    varchar
), '') || '-' || coalesce(cast(county_nm as 
    varchar
), '') as 
    varchar
)) as opco_locn_ck, 
            * 
    from pre_final
)

select * from final



    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_locn ) 
    
