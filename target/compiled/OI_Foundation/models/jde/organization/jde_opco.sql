

with jde_opco as(
    select 
    current_timestamp as crt_dtm,
    null::timestamp_tz as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    concat_ws('|','JDE-HNCK', ccco)::varchar(255) as opco_ak,
    'JDE-HNCK'::varchar(20) as src_sys_nm,
    trim(f0.ccco)::varchar(15) as opco_id,
    trim(f0.ccname)::varchar(120) as opco_nm,
    oc.opco_currency_sk,
    null::varchar(32) as region_sk,
    null::varchar(32) as sub_region_sk,
    trim(f0.ccaltc)::varchar(120) as dba_nm,
    '0'::number(1,0) as enterprise_ind,
    trim(f1.abtax)::varchar(130) as govt_issued_id,
    trim(f1.abduns)::varchar(15) as  duns_nbr,
    null::varchar(40) as vat_nbr,
    '2021-03-12'::date as acqstn_dt,
    '1'::number(1,0) as actv_ind,
    null::date as inactv_dt,
    null::varchar(40) as import_vat_nbr,
    trim(f0.ccan8) as website_url_txt_join,
    trim(f1.ablngp)::varchar(20) as language_txt,
    null::date as conversion_dt,
    '0'::number(1,0) as foreign_enty_ind,
    null::number(1,0) as combined_fed_state_tax_filer_ind,
    null::number(1,0) as validate_1099_on_entry_ind,
    null::number(1,0) as planning_company_ind,
    null::varchar(60) as sls_tax_global_id,
    null::varchar(60) as sls_tax_local_id,
    null::varchar(20) as product_line_txt,
    null::varchar(50) as prnt_opco_id,
    null::varchar(50) as collection_type_txt,
    null::number(1,0) as dashboard_usage_id
    from MULE_STAGING.JDE_HANCOCK.JDE_PRODUCTION_PRODDTA_F0010 as f0
    left join MULE_STAGING.JDE_HANCOCK.JDE_PRODUCTION_PRODDTA_F0101 as f1
        on trim(f0.ccan8) = trim(f1.aban8)
    left join OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_currency oc 
        on upper(trim(f0.cccrcd)) = oc.src_currency_cd
        and oc.src_sys_nm = 'JDE_HNCK'
),
website_url_col as(
    select 
    jde_opco.*,
    trim(f11.eaemal)::varchar(510) as website_url_txt
    from jde_opco
    left join MULE_STAGING.JDE_HANCOCK.JDE_PRODUCTION_PRODDTA_F01151 as f11
        on jde_opco.website_url_txt_join = trim(f11.eaan8)
        and trim(f11.eaidln) = 0 
        and trim(f11.eaetp) = 'I'
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
    crt_dtm, stg_load_dtm, delete_dtm, opco_ak, src_sys_nm, opco_id, opco_nm, opco_currency_sk, region_sk, sub_region_sk, dba_nm, enterprise_ind, govt_issued_id, duns_nbr, vat_nbr, acqstn_dt, actv_ind, inactv_dt, import_vat_nbr, website_url_txt, language_txt, conversion_dt, foreign_enty_ind, combined_fed_state_tax_filer_ind, validate_1099_on_entry_ind, planning_company_ind, sls_tax_global_id, sls_tax_local_id, product_line_txt, prnt_opco_id, collection_type_txt, dashboard_usage_id
    from website_url_col  
),
delete_captured as (
    select * from final
    
        union
        select
        OPCO_SK, OPCO_CK, opco_ak, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, opco_id, opco_nm, opco_currency_sk, region_sk, sub_region_sk, dba_nm, enterprise_ind, govt_issued_id,  duns_nbr, vat_nbr, acqstn_dt, actv_ind, inactv_dt, import_vat_nbr, website_url_txt, language_txt, conversion_dt, foreign_enty_ind, combined_fed_state_tax_filer_ind, validate_1099_on_entry_ind, planning_company_ind, sls_tax_global_id, sls_tax_local_id, product_line_txt, prnt_opco_id, collection_type_txt, dashboard_usage_id
        from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO
        
            where OPCO_ck not in (select distinct OPCO_ck from final)
         
        and src_sys_nm = 'JDE-HNCK'
    
)

select * from delete_captured



    
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco ) and OPCO_ck not in (select distinct OPCO_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO where src_sys_nm = 'JDE-HNCK')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco ) and OPCO_ck in (select distinct OPCO_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO where src_sys_nm = 'JDE-HNCK') and delete_dtm is not null)
    
