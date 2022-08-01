{% set _load = load_type('AX_OPCO') %}

with ax_opco as(
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
    from {{ source('AX_DEV', 'COMPANYINFO')}} ci
    left join {{ref('ax_region_curr')}} r 
        on upper(ci.region_opi) = r.region_id
        and r.src_sys_nm = 'AX'
    left join {{ref('ax_sub_region_curr')}} sr 
        on upper(ci.subregion_opi) = sr.sub_region_id
        and sr.src_sys_nm = 'AX'
    left join {{ref('ax_opco_currency_curr')}} oc 
        on upper(ci.currencycode) = oc.src_currency_cd
        and oc.src_sys_nm = 'AX'
    where dataareaid not in {{ var('excluded_ax_companies')}}
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id']) }} as opco_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'opco_nm', 'opco_currency_sk', 'region_sk', 'sub_region_sk', 'dba_nm', 'enterprise_ind', 'govt_issued_id', ' duns_nbr', 'vat_nbr', 'acqstn_dt', 'actv_ind', 'inactv_dt', 'import_vat_nbr', 'website_url_txt', 'language_txt', 'conversion_dt', 'foreign_enty_ind', 'combined_fed_state_tax_filer_ind', 'validate_1099_on_entry_ind', 'planning_company_ind', 'sls_tax_global_id', 'sls_tax_local_id', 'product_line_txt', 'prnt_opco_id', 'collection_type_txt', 'dashboard_usage_id']) }} as opco_ck,
            *
    from ax_opco 
    
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO') and _load != 3%}
    union
    select
    opco_sk, opco_ck, CRT_DTM, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    opco_ak, src_sys_nm, opco_id, opco_nm, opco_currency_sk, region_sk, sub_region_sk, dba_nm, enterprise_ind, govt_issued_id,  duns_nbr, vat_nbr, acqstn_dt, actv_ind, inactv_dt, import_vat_nbr, website_url_txt, language_txt, conversion_dt, foreign_enty_ind, combined_fed_state_tax_filer_ind, validate_1099_on_entry_ind, planning_company_ind, sls_tax_global_id, sls_tax_local_id, product_line_txt, prnt_opco_id, collection_type_txt, dashboard_usage_id
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO
    {% if _load == 1 %}
        where opco_ck not in (select distinct opco_ck from final)
    {% else %}
        where opco_ck not in (select distinct opco_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_ck not in (select distinct opco_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_ck in (select distinct opco_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  