{% set _load = load_type('AX_OPCO_CUST_OPCO_LOCN_XREF') %}

with ax_opco_cust_opco_locn_xref as(
    select
    oc.opco_cust_sk, 
    ol.opco_locn_sk,
    'MAIN' as opco_cust_opco_locn_asscn_type_txt,
    current_timestamp as crt_dtm,
    cp._fivetran_synced as stg_load_dtm,
    case
        when cp._fivetran_deleted = true then cp._fivetran_synced 
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    cp.function_ as contact_type_txt,
    cp.name as contact_nm,
    cp.phone as contact_ph_nbr,
    '' as contact_ph_extn_nbr,
    cp.cellularphone as contact_cell_nbr,
    cp.phonelocal as contact_local_ph_nbr,
    cp.telefax as contact_fax_nbr,
    '' as contact_telex_nbr,
    cp.email as contact_email_id,
    upper(cp.nativelanguage) as language_txt,
    '' as start_dt,
    '' as end_dt,
    cp.leftcompany as actv_ind
    from {{source('AX_DEV', 'CONTACTPERSON')}} cp
    left join {{source('AX_DEV', 'CUSTTABLE')}} ct
        on cp.contactpersonid = ct.contactpersonid
        and cp.dataareaid = ct.dataareaid
        and cp.dataareaid not in {{ var('excluded_ax_companies')}}
        and ct.dataareaid not in {{ var('excluded_ax_companies')}}
        and ct._fivetran_deleted = false
    left join {{ref('ax_opco_locn_curr')}} ol
        on upper(ct.street) = ol.addr_ln_1_txt
        and upper(ct.city) = ol.city_nm
        and upper(ct.state) = ol.state_nm
        and upper(ct.countryregionid) = ol.country_nm
        and upper(ct.zipcode) = ol.zip_cd
        and ol.src_sys_nm = 'AX'
    left join {{ref('ax_opco_cust_curr')}} oc 
        on ct.accountnum = oc.cust_id
        and ct.dataareaid = oc.opco_id
        and oc.src_sys_nm = 'AX'
),
final as (
    select distinct 
            {{dbt_utils.surrogate_key(['opco_cust_sk', 'opco_locn_sk',  'opco_cust_opco_locn_asscn_type_txt',  'src_sys_nm',  'contact_type_txt',  'contact_nm',  'contact_ph_nbr',  'contact_ph_extn_nbr',  'contact_cell_nbr',  'contact_local_ph_nbr',  'contact_fax_nbr',  'contact_telex_nbr',  'contact_email_id',  'language_txt',  'start_dt',  'end_dt',  'actv_ind'])}} as opco_cust_opco_locn_xref_ck,
            * 
    from ax_opco_cust_opco_locn_xref
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_CUST_OPCO_LOCN_XREF') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CUST_OPCO_LOCN_XREF') and _load != 3%}
    union
    select
    opco_cust_opco_locn_xref_ck, opco_cust_sk, opco_locn_sk, opco_cust_opco_locn_asscn_type_txt, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, contact_type_txt, contact_nm, contact_ph_nbr, contact_ph_extn_nbr, contact_cell_nbr, contact_local_ph_nbr, contact_fax_nbr, contact_telex_nbr, contact_email_id, language_txt, start_dt, end_dt, actv_ind
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CUST_OPCO_LOCN_XREF
    {% if _load == 1 %}
        where opco_cust_opco_locn_xref_ck not in (select distinct opco_cust_opco_locn_xref_ck from final)
    {% else %}
        where opco_cust_opco_locn_xref_ck not in (select distinct opco_cust_opco_locn_xref_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CUST_OPCO_LOCN_XREF where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}

{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_opco_locn_xref_ck not in (select distinct opco_cust_opco_locn_xref_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CUST_OPCO_LOCN_XREF where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_opco_locn_xref_ck in (select distinct opco_cust_opco_locn_xref_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CUST_OPCO_LOCN_XREF where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}