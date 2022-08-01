

with  __dbt__cte__v_ns_opco_currency_curr as (
with v_ns_opco_currency as(
    select *,
    rank() over(partition by opco_currency_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_currency
),
final as(
    select 
    *
    from v_ns_opco_currency
    where rnk = 1 and delete_dtm is null
)

select * from final
),ns_opco as(
    select 
    current_timestamp as crt_dtm,
    sd._fivetran_synced as stg_load_dtm,
    case
        when sd._fivetran_deleted = true then sd._fivetran_synced
        else null
    end as delete_dtm,
    concat_ws('|', 'NS', sd.subsidiary_id)::varchar(255) as opco_ak,
    'NS'::varchar(20) as src_sys_nm,
    sd.subsidiary_id::varchar(15) as opco_id,
    sd.name::varchar(120) as opco_nm,
    ocr.opco_currency_sk,
    null::varchar(32) as region_sk,
    null::varchar(32) as sub_region_sk,
    null::varchar(120) as dba_nm,
    null::numeric(1,0) as enterprise_ind,
    sd.federal_number::varchar(130) as govt_issued_id,
    null::varchar(15) as duns_nbr,
    null::varchar(40) as vat_nbr,
    null::date as acqstn_dt,
    case 
        when sd.isinactive='No' then 1
        when sd.isinactive='Yes' then 0  
    end::numeric(1,0) as actv_ind,
    null::date as inactv_dt,
    null::varchar(40) as import_vat_nbr,
    sd.url::varchar(510) as website_url_txt,
    null::varchar(20) as language_txt,
    null::date as conversion_dt,
    null::numeric(1,0) as foreign_enty_ind,
    null::numeric(1,0) as combined_fed_state_tax_filer_ind,
    null::numeric(1,0) as validate_1099_on_entry_ind,
    null::numeric(1,0) as planning_company_ind,
    null::varchar(60) as sls_tax_global_id,
    null::varchar(60) as sls_tax_local_id,
    null::varchar(20) as product_line_txt,
    null::varchar(50) as prnt_opco_id,
    null::varchar(50) as collection_type_txt,
    null::numeric(1,0) as dashboard_usage_id 
    from FIVETRAN.NETSUITE.SUBSIDIARIES sd
    left join __dbt__cte__v_ns_opco_currency_curr ocr 
       on upper(sd.base_currency_id) = ocr.src_currency_cd
       and ocr.src_sys_nm = 'NS'
    where subsidiary_id not in (1, 4, 5, 7)
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') as 
    varchar
)) as opco_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(opco_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(region_sk as 
    varchar
), '') || '-' || coalesce(cast(sub_region_sk as 
    varchar
), '') || '-' || coalesce(cast(dba_nm as 
    varchar
), '') || '-' || coalesce(cast(enterprise_ind as 
    varchar
), '') || '-' || coalesce(cast(govt_issued_id as 
    varchar
), '') || '-' || coalesce(cast(duns_nbr as 
    varchar
), '') || '-' || coalesce(cast(vat_nbr as 
    varchar
), '') || '-' || coalesce(cast(acqstn_dt as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') || '-' || coalesce(cast(inactv_dt as 
    varchar
), '') || '-' || coalesce(cast(import_vat_nbr as 
    varchar
), '') || '-' || coalesce(cast(website_url_txt as 
    varchar
), '') || '-' || coalesce(cast(language_txt as 
    varchar
), '') || '-' || coalesce(cast(conversion_dt as 
    varchar
), '') || '-' || coalesce(cast(foreign_enty_ind as 
    varchar
), '') || '-' || coalesce(cast(combined_fed_state_tax_filer_ind as 
    varchar
), '') || '-' || coalesce(cast(validate_1099_on_entry_ind as 
    varchar
), '') || '-' || coalesce(cast(planning_company_ind as 
    varchar
), '') || '-' || coalesce(cast(sls_tax_global_id as 
    varchar
), '') || '-' || coalesce(cast(sls_tax_local_id as 
    varchar
), '') || '-' || coalesce(cast(product_line_txt as 
    varchar
), '') || '-' || coalesce(cast(prnt_opco_id as 
    varchar
), '') || '-' || coalesce(cast(collection_type_txt as 
    varchar
), '') || '-' || coalesce(cast(dashboard_usage_id as 
    varchar
), '') as 
    varchar
)) as opco_ck,
            *
    from ns_opco  
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco ) 
    
