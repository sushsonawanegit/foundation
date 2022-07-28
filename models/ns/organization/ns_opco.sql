{% set _load = load_type('NS_OPCO') %}

with ns_opco as(
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
    from {{ source('NS_DEV', 'SUBSIDIARIES')}} sd
    left join {{ref('opco_currency')}} ocr 
       on upper(sd.base_currency_id) = ocr.src_currency_cd
       and ocr.src_sys_nm = 'NS'
    where subsidiary_id not in (1, 4, 5, 7)
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id']) }} as opco_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'opco_nm', 'opco_currency_sk', 'region_sk', 'sub_region_sk', 'dba_nm', 'enterprise_ind', 'govt_issued_id', 'duns_nbr', 'vat_nbr', 'acqstn_dt', 'actv_ind', 'inactv_dt', 'import_vat_nbr', 'website_url_txt', 'language_txt', 'conversion_dt', 'foreign_enty_ind', 'combined_fed_state_tax_filer_ind', 'validate_1099_on_entry_ind', 'planning_company_ind', 'sls_tax_global_id', 'sls_tax_local_id', 'product_line_txt', 'prnt_opco_id', 'collection_type_txt', 'dashboard_usage_id']) }} as opco_ck,
            *
    from ns_opco  
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'NS_OPCO') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO') and _load != 3%}
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
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO where src_sys_nm = 'NS')
    {% endif %} 
    and src_sys_nm = 'NS'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ck not in (select distinct OPCO_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO where src_sys_nm = 'NS')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ck in (select distinct OPCO_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO where src_sys_nm = 'NS') and delete_dtm is not null)
    {% endif %}
{% endif %}  