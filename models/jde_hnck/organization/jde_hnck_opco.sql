{% set _load = load_type('JDE_HNCK_OPCO') %}

with jde_hnck_opco as(
    select 
    current_timestamp as crt_dtm,
    f0.mule_load_ts as stg_load_dtm,
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
    from {{source('JDE_HNCK_DEV1','F0010')}} as f0
    left join {{source('JDE_HNCK_DEV1','F0101')}} as f1
        on trim(f0.ccan8) = trim(f1.aban8)
    left join {{ ref('jde_hnck_opco_currency')}} oc 
        on upper(trim(f0.cccrcd)) = oc.src_currency_cd
        and oc.src_sys_nm = 'JDE_HNCK'
),
website_url_col as(
    select 
    jde_hnck_opco.*,
    trim(f11.eaemal)::varchar(510) as website_url_txt
    from jde_hnck_opco
    left join {{source('JDE_HNCK_DEV1','F01151')}} as f11
        on jde_hnck_opco.website_url_txt_join = trim(f11.eaan8)
        and trim(f11.eaidln) = 0 
        and trim(f11.eaetp) = 'I'
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id']) }} as opco_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'opco_nm', 'opco_currency_sk', 'region_sk', 'sub_region_sk', 'dba_nm', 'enterprise_ind', 'govt_issued_id', 'duns_nbr', 'vat_nbr', 'acqstn_dt', 'actv_ind', 'inactv_dt', 'import_vat_nbr', 'website_url_txt', 'language_txt', 'conversion_dt', 'foreign_enty_ind', 'combined_fed_state_tax_filer_ind', 'validate_1099_on_entry_ind', 'planning_company_ind', 'sls_tax_global_id', 'sls_tax_local_id', 'product_line_txt', 'prnt_opco_id', 'collection_type_txt', 'dashboard_usage_id']) }} as opco_ck, 
    crt_dtm, stg_load_dtm, delete_dtm, opco_ak, src_sys_nm, opco_id, opco_nm, opco_currency_sk, region_sk, sub_region_sk, dba_nm, enterprise_ind, govt_issued_id, duns_nbr, vat_nbr, acqstn_dt, actv_ind, inactv_dt, import_vat_nbr, website_url_txt, language_txt, conversion_dt, foreign_enty_ind, combined_fed_state_tax_filer_ind, validate_1099_on_entry_ind, planning_company_ind, sls_tax_global_id, sls_tax_local_id, product_line_txt, prnt_opco_id, collection_type_txt, dashboard_usage_id
    from website_url_col  
),
delete_captured as (
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'JDE_HNCK_OPCO') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO') and _load != 3%}
        union
        select
        OPCO_SK, OPCO_CK, opco_ak, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, opco_id, opco_nm, opco_currency_sk, region_sk, sub_region_sk, dba_nm, enterprise_ind, govt_issued_id,  duns_nbr, vat_nbr, acqstn_dt, actv_ind, inactv_dt, import_vat_nbr, website_url_txt, language_txt, conversion_dt, foreign_enty_ind, combined_fed_state_tax_filer_ind, validate_1099_on_entry_ind, planning_company_ind, sls_tax_global_id, sls_tax_local_id, product_line_txt, prnt_opco_id, collection_type_txt, dashboard_usage_id
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO
        {% if _load == 1 %}
            where OPCO_ck not in (select distinct OPCO_ck from final)
        {% else %}
            where OPCO_ck not in (select distinct OPCO_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO where src_sys_nm = 'JDE-HNCK')
        {% endif %} 
        and src_sys_nm = 'JDE-HNCK'
    {% endif %}
)

select * from delete_captured


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ck not in (select distinct OPCO_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO where src_sys_nm = 'JDE-HNCK')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ck in (select distinct OPCO_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO where src_sys_nm = 'JDE-HNCK') and delete_dtm is not null)
    {% endif %}
{% endif %}  