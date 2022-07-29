{% set _load = load_type('AX_OPCO_OPCO_LOCN_XREF') %}

with ax_opco_opco_locn_xref as (
    select
    opco.opco_sk,
    ol.opco_locn_sk,
    'MAIN' as opco_opco_locn_asscn_type_txt,
    current_timestamp as crt_dtm,
    ci._fivetran_synced as stg_load_dtm,
    case
        when ci._fivetran_deleted = true then ci._fivetran_synced 
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    '' as contact_type_txt,
    '' as contact_nm,
    ci.phone as contact_ph_nbr,
    '' as contact_ph_extn_nbr,
    ci.cellularphone as contact_cell_nbr,
    ci.phonelocal as contact_local_ph_nbr,
    ci.telefax as contact_fax_nbr,
    '' as contact_telex_nbr,
    ci.email as contact_email_id,
    upper(ci.languageid) as language_txt,
    '' as start_dt,
    '' as end_dt
    from {{source('AX_DEV', 'COMPANYINFO')}} ci
    left join {{ref('v_ax_opco_curr')}} opco 
        on ci.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ref('v_ax_opco_locn_curr')}} ol 
        on upper(ci.street) = ol.addr_ln_1_txt
        and upper(ci.city) = ol.city_nm
        and upper(ci.state) = ol.state_nm
        and upper(ci.countryregionid) = ol.country_nm
        and upper(ci.zipcode) = ol.zip_cd 
        and ol.src_sys_nm = 'AX'
    where ci.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as(
    select distinct 
        {{dbt_utils.surrogate_key(['opco_sk', 'opco_locn_sk', 'opco_opco_locn_asscn_type_txt', 'contact_type_txt', 'contact_nm', 'contact_ph_nbr', 'contact_ph_extn_nbr', 'contact_cell_nbr', 'contact_local_ph_nbr', 'contact_fax_nbr', 'contact_telex_nbr', 'contact_email_id', 'language_txt', 'start_dt', 'end_dt'])}} as opco_opco_locn_xref_ck, 
        * 
    from ax_opco_opco_locn_xref 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_OPCO_LOCN_XREF') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_OPCO_LOCN_XREF') and _load != 3%}
    union
    select
    opco_opco_locn_xref_ck, opco_sk, opco_locn_sk, opco_opco_locn_asscn_type_txt, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, contact_type_txt, contact_nm, contact_ph_nbr, contact_ph_extn_nbr, contact_cell_nbr, contact_local_ph_nbr, contact_fax_nbr, contact_telex_nbr, contact_email_id, language_txt, start_dt, end_dt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_OPCO_LOCN_XREF
    {% if _load == 1 %}
        where opco_opco_locn_xref_ck not in (select distinct opco_opco_locn_xref_ck from final)
    {% else %}
        where opco_opco_locn_xref_ck not in (select distinct opco_opco_locn_xref_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_OPCO_LOCN_XREF where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}

{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_opco_locn_xref_ck not in (select distinct opco_opco_locn_xref_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_OPCO_LOCN_XREF where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_opco_locn_xref_ck in (select distinct opco_opco_locn_xref_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_OPCO_LOCN_XREF where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}