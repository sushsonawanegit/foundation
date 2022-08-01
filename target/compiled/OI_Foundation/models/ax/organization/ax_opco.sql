

with  __dbt__cte__v_ax_region_curr as (
with v_ax_region as(
    select *,
    rank() over(partition by region_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_region
),
final as(
    select 
    *
    from v_ax_region
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_sub_region_curr as (
with v_ax_sub_region as(
    select *,
    rank() over(partition by sub_region_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_sub_region
),
final as(
    select 
    *
    from v_ax_sub_region
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_currency_curr as (
with v_ax_opco_currency as(
    select *,
    rank() over(partition by opco_currency_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_currency
),
final as(
    select 
    *
    from v_ax_opco_currency
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco as(
    select 
    current_timestamp as crt_dtm,
    ci._fivetran_synced as stg_load_dtm,
    case
        when ci._fivetran_deleted = true then ci._fivetran_synced
        else null
    end as delete_dtm,
    concat_ws('|', 'AX', ci.dataareaid)::varchar(255) as opco_ak,
    'AX'::varchar(20) as src_sys_nm,
    ci.dataareaid::varchar(15) as opco_id,
    ci.name::varchar(120) as opco_nm,
    oc.opco_currency_sk,
    r.region_sk,
    sr.sub_region_sk,
    ci.dba::varchar(120) as dba_nm,
    null::numeric(1,0) as enterprise_ind,
    null::varchar(130) as govt_issued_id,
    null::varchar(15) as  duns_nbr,
    ci.vatnum::varchar(20) as vat_nbr,
    null::date as acqstn_dt,
    iff(ci.companyclosed_opi=0, 1, 0)::numeric(1,0) as actv_ind,
    null::date as inactv_dt,
    ci.importvatnum::varchar(40) as import_vat_nbr,
    ci.url::varchar(510) as website_url_txt,
    ci.languageid::varchar(20) as language_txt,
    ci.conversiondate::date as conversion_dt,
    ci.foreignentityindicator::numeric(1,0) as foreign_enty_ind,
    ci.combinedfedstatefiler::numeric(1,0) as combined_fed_state_tax_filer_ind,
    ci.validate1099onentry::numeric(1,0) as validate_1099_on_entry_ind,
    ci.planningcompany::numeric(1,0) as planning_company_ind,
    null::varchar(60) as sls_tax_global_id,
    null::varchar(60) as sls_tax_local_id,
    ci.productline_opi::varchar(20) as product_line_txt,
    concat_ws('|', 'AX', ci.currentcompany_opi)::varchar(50) as prnt_opco_id,
    ci.collectiontype_opi::varchar(50) as collection_type_txt,
    ci.dashboard_opi::numeric(1,0) as dashboard_usage_id
    from FIVETRAN.AX_PRODREPLICATION1_DBO.COMPANYINFO ci
    left join __dbt__cte__v_ax_region_curr r 
        on upper(ci.region_opi) = r.region_id
        and r.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_sub_region_curr sr 
        on upper(ci.subregion_opi) = sr.sub_region_id
        and sr.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr oc 
        on upper(ci.currencycode) = oc.src_currency_cd
        and oc.src_sys_nm = 'AX'
    where dataareaid not in ('044', '045', '047', '999')
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
), '') || '-' || coalesce(cast( duns_nbr as 
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
    from ax_opco 
    
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco ) 
    
