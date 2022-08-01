{% set _load = load_type('AX_SLS_ORDR_OPCO_LOCN_XREF') %}

with ax_sls_ordr_opco_locn_xref as (
    select 
    {{ dbt_utils.surrogate_key(["'AX'", 'st.dataareaid', 'st.salesid']) }} as opco_sls_ordr_sk,
    ol.opco_locn_sk,
    'DELIVERY' as sls_ordr_opco_locn_asscn_type_txt,
    current_timestamp as crt_dtm,
    cp._fivetran_synced as stg_load_dtm,
    case
        when cp._fivetran_deleted = true then cp._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    '' as contact_type_txt,
    cp.name as contact_nm,
    cp.phone as contact_ph_nbr,
    '' as contact_ph_extn_nbr,
    cp.cellularphone as contact_cell_nbr,
    cp.phonelocal as contact_local_ph_nbr,
    cp.telefax as contact_fax_nbr,
    cp.telex as contact_telex_nbr,
    cp.email as contact_email_id,
    upper(st.languageid) as languate_txt,
    '' as start_dt,
    '' as end_dt
    from {{source('AX_DEV', 'SALESTABLE')}} st
    left join {{source('AX_DEV', 'CONTACTPERSON')}} cp  
        on cp.dataareaid = st.dataareaid
        and cp.contactpersonid = st.contactpersonid
        and cp.dataareaid not in {{ var('excluded_ax_companies')}}
        and st.dataareaid not in {{ var('excluded_ax_companies')}}
        and cp._fivetran_deleted = false
    left join {{ref('ax_opco_locn_curr')}} ol 
        on upper(st.deliverystreet) = ol.addr_ln_1_txt 
        and upper(st.deliverycity) = ol.city_nm
        and upper(st.deliverystate) = ol.state_nm
        and upper(st.deliverycountryregionid) = ol.country_nm
        and upper(st.deliveryzipcode) = ol.zip_cd
        and ol.src_sys_nm = 'AX'
),
final as (
    select 
        opco_sls_ordr_sk, opco_locn_sk, sls_ordr_opco_locn_asscn_type_txt,
        {{ dbt_utils.surrogate_key(['opco_sls_ordr_sk', 'opco_locn_sk', 'sls_ordr_opco_locn_asscn_type_txt', 'src_sys_nm', 'contact_type_txt', 'contact_nm', 'contact_ph_nbr', 'contact_ph_extn_nbr', 'contact_cell_nbr', 'contact_local_ph_nbr', 'contact_fax_nbr', 'contact_telex_nbr', 'contact_email_id', 'languate_txt', 'start_dt', 'end_dt'])}} as sls_ordr_opco_locn_xref_ck,
        crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, contact_type_txt, contact_nm, contact_ph_nbr, contact_ph_extn_nbr, contact_cell_nbr, contact_local_ph_nbr, contact_fax_nbr, contact_telex_nbr, contact_email_id, languate_txt, start_dt, end_dt
    from ax_sls_ordr_opco_locn_xref 
)

select * from final   
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_SLS_ORDR_OPCO_LOCN_XREF') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'SLS_ORDR_OPCO_LOCN_XREF') and _load != 3%}
    union
    select
    opco_sls_ordr_sk, opco_locn_sk, sls_ordr_opco_locn_asscn_type_txt, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, contact_type_txt, contact_nm, contact_ph_nbr, contact_ph_extn_nbr, contact_cell_nbr, contact_local_ph_nbr, contact_fax_nbr, contact_telex_nbr, contact_email_id, languate_txt, start_dt, end_dt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.SLS_ORDR_OPCO_LOCN_XREF
    {% if _load == 1 %}
        where SLS_ORDR_OPCO_LOCN_XREF_ck not in (select distinct SLS_ORDR_OPCO_LOCN_XREF_ck from final)
    {% else %}
        where SLS_ORDR_OPCO_LOCN_XREF_ck not in (select distinct SLS_ORDR_OPCO_LOCN_XREF_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.SLS_ORDR_OPCO_LOCN_XREF where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and SLS_ORDR_OPCO_LOCN_XREF_ck not in (select distinct SLS_ORDR_OPCO_LOCN_XREF_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.SLS_ORDR_OPCO_LOCN_XREF where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and SLS_ORDR_OPCO_LOCN_XREF_ck in (select distinct SLS_ORDR_OPCO_LOCN_XREF_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.SLS_ORDR_OPCO_LOCN_XREF where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}