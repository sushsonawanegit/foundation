{% set _load = load_type('AX_OPCO_VENDOR_OPCO_LOCN_XREF') %}

with ax_opco_vendor_opco_locn_xref as(
    select 
    ov.opco_vendor_sk,
    ol.opco_locn_sk,
    'MAIN' as opco_vendor_opco_locn_asscn_type_txt,
    current_timestamp as crt_dtm,
    vt._fivetran_synced as stg_load_dtm,
    case
        when vt._fivetran_deleted = true then vt._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    '' as contact_type_txt,
    '' as contact_nm,
    vt.phone as contact_ph_nbr,
    '' as contact_ph_extn_nbr,
    vt.cellularphone as contact_cell_nbr,
    vt.phonelocal as contact_local_ph_nbr,
    vt.telefax as contact_fax_nbr,
    vt.telex as contact_telex_nbr,
    vt.email as contact_email_id,
    upper(vt.languageid) as language_txt,
    '' as start_dt,
    '' as end_dt
    from {{source('AX_DEV', 'VENDTABLE')}} vt
    left join {{ref('opco_locn')}} ol 
        on upper(vt.street) = ol.addr_ln_1_txt
        and upper(vt.city) = ol.city_nm
        and upper(vt.state) = ol.state_nm
        and upper(vt.countryregionid) = ol.country_nm
        and upper(vt.zipcode) = ol.zip_cd 
        and ol.src_sys_nm = 'AX'
    left join {{ref('opco_vendor')}} ov 
        on upper(vt.accountnum) = ov.vendor_id
        and ov.src_sys_nm = 'AX'
    where vt.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as(
    select distinct 
            {{ dbt_utils.surrogate_key([ 'opco_vendor_sk',  'opco_locn_sk',  'opco_vendor_opco_locn_asscn_type_txt',  'src_sys_nm',  'contact_type_txt',  'contact_nm',  'contact_ph_nbr',  'contact_ph_extn_nbr',  'contact_cell_nbr',  'contact_local_ph_nbr',  'contact_fax_nbr',  'contact_telex_nbr',  'contact_email_id',  'language_txt',  'start_dt',  'end_dt'])}} as opco_vendor_opco_locn_xref_ck,
            * 
    from ax_opco_vendor_opco_locn_xref
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_CUST_OPCO_LOCN_XREF') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CUST_OPCO_LOCN_XREF') and _load != 3%}
    union
    select
    opco_vendor_opco_locn_xref_ck, opco_vendor_sk, opco_locn_sk, opco_vendor_opco_locn_asscn_type_txt, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, contact_type_txt, contact_nm, contact_ph_nbr, contact_ph_extn_nbr, contact_cell_nbr, contact_local_ph_nbr, contact_fax_nbr, contact_telex_nbr, contact_email_id, language_txt, start_dt, end_dt
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