{% set _load = load_type('AX_OPCO_CUST') %}

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
    from {{ source('AX_DEV', 'CUSTTABLE') }} 
    where dataareaid not in {{ var('excluded_ax_companies')}}
),
final as(
    select 
        {{dbt_utils.surrogate_key(['src_sys_nm', 'cust_id', 'opco_id'])}} as opco_cust_sk,
        {{dbt_utils.surrogate_key(['src_sys_nm', 'cust_id', 'cust_nm','opco_id','dba_nm','govt_issued_id','duns_nbr','lob_nm','vat_number','language_txt','country_cd','website_url_txt','tax_exempt_nbr','tax_grp_txt','cust_item_grp_cd','end_market_nm','collector_cd','party_id','flag_id'])}} as opco_cust_ck, 
        *
    from ax_opco_cust
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_CUST') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CUST') and _load != 3%}
    union
    select
    opco_cust_sk, opco_cust_ck, opco_cust_ak, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, cust_id, opco_id, dba_nm, govt_issued_id, duns_nbr, lob_nm, vat_number, language_txt, country_cd, website_url_txt, tax_exempt_nbr, tax_grp_txt, cust_item_grp_cd, end_market_nm, collector_cd, party_id, flag_id
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust
    {% if _load == 1 %}
        where opco_cust_ck not in (select distinct opco_cust_ck from final)
    {% else %}
        where opco_cust_ck not in (select distinct opco_cust_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_ck not in (select distinct opco_cust_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_ck in (select distinct opco_cust_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cust where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}