

with  __dbt__cte__v_ax_opco_locn_curr as (
with v_ax_opco_locn as(
    select *,
    rank() over(partition by opco_locn_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_locn
),
final as(
    select 
    *
    from v_ax_opco_locn
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_vendor_curr as (
with v_ax_opco_vendor as(
    select *,
    rank() over(partition by opco_vendor_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor
),
final as(
    select 
    *
    from v_ax_opco_vendor
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_vendor_opco_locn_xref as(
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
    from FIVETRAN.AX_PRODREPLICATION1_DBO.VENDTABLE vt
    left join __dbt__cte__v_ax_opco_locn_curr ol 
        on upper(vt.street) = ol.addr_ln_1_txt
        and upper(vt.city) = ol.city_nm
        and upper(vt.state) = ol.state_nm
        and upper(vt.countryregionid) = ol.country_nm
        and upper(vt.zipcode) = ol.zip_cd 
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_vendor_curr ov 
        on upper(vt.accountnum) = ov.vendor_id
        and ov.src_sys_nm = 'AX'
    where vt.dataareaid not in ('044', '045', '047', '999')
),
final as(
    select distinct 
            md5(cast(coalesce(cast(opco_vendor_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_locn_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_vendor_opco_locn_asscn_type_txt as 
    varchar
), '') || '-' || coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(contact_type_txt as 
    varchar
), '') || '-' || coalesce(cast(contact_nm as 
    varchar
), '') || '-' || coalesce(cast(contact_ph_nbr as 
    varchar
), '') || '-' || coalesce(cast(contact_ph_extn_nbr as 
    varchar
), '') || '-' || coalesce(cast(contact_cell_nbr as 
    varchar
), '') || '-' || coalesce(cast(contact_local_ph_nbr as 
    varchar
), '') || '-' || coalesce(cast(contact_fax_nbr as 
    varchar
), '') || '-' || coalesce(cast(contact_telex_nbr as 
    varchar
), '') || '-' || coalesce(cast(contact_email_id as 
    varchar
), '') || '-' || coalesce(cast(language_txt as 
    varchar
), '') || '-' || coalesce(cast(start_dt as 
    varchar
), '') || '-' || coalesce(cast(end_dt as 
    varchar
), '') as 
    varchar
)) as opco_vendor_opco_locn_xref_ck,
            * 
    from ax_opco_vendor_opco_locn_xref
)

select * from final



    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_opco_locn_xref ) 
    
