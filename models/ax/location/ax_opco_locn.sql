{% set _load = load_type('AX_OPCO_LOCN') %}

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
    from {{source('AX_DEV', 'ADDRESS')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
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
    from {{source('AX_DEV', 'COMPANYINFO')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
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
    from {{source('AX_DEV', 'CUSTTABLE')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
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
    from {{source('AX_DEV', 'SALESTABLE')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
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
    from {{source('AX_DEV', 'VENDTABLE')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
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
    from {{source('AX_DEV', 'PROJTABLE')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
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
    from {{source('AX_DEV', 'CUSTINVOICEJOUR')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
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
    from {{source('AX_DEV', 'CUSTINVOICEJOUR')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
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
    from {{source('AX_DEV', 'CUSTPACKINGSLIPJOUR')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
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
    from {{source('AX_DEV', 'PURCHTABLE')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
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
    from {{source('AX_DEV', 'VENDPACKINGSLIPJOUR')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
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
            {{dbt_utils.surrogate_key(['src_sys_nm', 'std_locn_sk', 'addr_ln_1_txt', 'addr_ln_2_txt', 'addr_ln_3_txt', 'city_nm', 'state_nm', 'country_nm', 'zip_cd'])}} as opco_locn_sk, 
            {{dbt_utils.surrogate_key(['src_sys_nm', 'std_locn_sk', 'addr_ln_1_txt', 'addr_ln_2_txt', 'addr_ln_3_txt', 'city_nm', 'state_nm', 'country_nm', 'zip_cd','county_nm'])}} as opco_locn_ck, 
            * 
    from pre_final
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_LOCN') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_LOCN') and _load != 3%}
    union
    select
    opco_locn_sk, opco_locn_ck, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, addr_ln_1_txt, addr_ln_2_txt, addr_ln_3_txt, city_nm, state_nm, state_nm, country_nm, zip_cd, county_nm
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOCN
    {% if _load == 1 %}
        where opco_locn_ck not in (select distinct opco_locn_ck from final)
    {% else %}
        where opco_locn_ck not in (select distinct opco_locn_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOCN where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}

{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_locn_ck not in (select distinct opco_locn_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOCN where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_locn_ck in (select distinct opco_locn_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOCN where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}