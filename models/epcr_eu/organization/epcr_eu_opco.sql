{% set _load = load_type('EPCR_EU_OPCO') %}

with epcr_eu_opco as(
    select 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then _fivetran_synced
        else null
    end as delete_dtm,
    'EPCR-EU' as src_sys_nm,
    upper(trim(company)) as opco_id,
    trim(name) as opco_nm,
    '' as region_sk,
    '' as sub_region_sk,
    '' as dba_nm,
    '' as enterprise_ind,
    co.statetaxid as govt_issued_id,
    '' as duns_nbr,
    '' as vat_nbr,
    '' as acqstn_dt,
    1 as actv_ind,
    '' as inactv_dt,
    '' as import_vat_nbr,
    '' as website_url_txt,
    '' as language_txt,
    '' as conversion_dt,
    '' as foreign_enty_ind,
    '' as combined_fed_state_tax_filer_ind,
    '' as validate_1099_on_entry_ind,
    '' as planning_company_ind,
    '' as sls_tax_global_id,
    '' as sls_tax_local_id,
    '' as product_line_txt,
    '' as prnt_opco_id,
    '' as collection_type_txt,
    '' as dashboard_usage_id
    from {{ source('EPCR_EU_DEV', 'COMPANY')}} co
       left join {{ref('epcr_eu_opco_currency_curr')}} cu
       on co.company = cu.company 
       and cu.basecurr = 'TRUE'
    where company not in ('CUBAUS01','CUBAUS02') 

),
final as(
    select 
        {{dbt_utils.surrogate_key(['src_sys_nm', 'opco_id'])}} as opco_sk,
        {{dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'opco_nm', 'region_sk', 'sub_region_sk', 'dba_nm', 'enterprise_ind', 'govt_issued_id', 'duns_nbr', 'vat_nbr', 'acqstn_dt', 'actv_ind', 'inactv_dt', 'import_vat_nbr', 'website_url_txt',
          'language_txt', 'conversion_dt', 'foreign_enty_ind', 'combined_fed_state_tax_filer_ind', 'validate_1099_on_entry_ind', 'planning_company_ind', 'sls_tax_global_id', 'sls_tax_local_id', 'product_line_txt', 'prnt_opco_id', 'collection_type_txt', 'dashboard_usage_id'])}} as opco_ck, 
        *
    from epcr_eu_opco

),
delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'EPCR_EU_OPCO') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO') and _load != 3%}
        union
        select
        OPCO_SK, OPCO_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, opco_id, opco_nm, region_sk, sub_region_sk, dba_nm, enterprise_ind, govt_issued_id, duns_nbr, vat_nbr, acqstn_dt, actv_ind, inactv_dt, import_vat_nbr, website_url_txt,
        language_txt, conversion_dt, foreign_enty_ind, combined_fed_state_tax_filer_ind, validate_1099_on_entry_ind, planning_company_ind, sls_tax_global_id, sls_tax_local_id, product_line_txt, prnt_opco_id, collection_type_txt, dashboard_usage_id
        
         
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco
        {% if _load == 1 %}
            where opco_ck not in (select distinct opco_ck from final)
        {% else %}
            where opco_ck not in (select distinct opco_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco where src_sys_nm = 'EPCR-EU')
        {% endif %} 
        and src_sys_nm = 'EPCR-EU'
    {% endif %}
)

select * from delete_captured


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_ck not in (select distinct opco_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco where src_sys_nm = 'EPCR-EU')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_ck in (select distinct opco_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco where src_sys_nm = 'EPCR-EU') and delete_dtm is not null)
    {% endif %}
{% endif %}


