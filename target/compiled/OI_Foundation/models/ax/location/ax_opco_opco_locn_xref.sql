

with  __dbt__cte__v_ax_opco_curr as (
with v_ax_opco as(
    select *,
    rank() over(partition by opco_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco
),
final as(
    select 
    *
    from v_ax_opco
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_locn_curr as (
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
),ax_opco_opco_locn_xref as (
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
    from FIVETRAN.AX_PRODREPLICATION1_DBO.COMPANYINFO ci
    left join __dbt__cte__v_ax_opco_curr opco 
        on ci.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_locn_curr ol 
        on upper(ci.street) = ol.addr_ln_1_txt
        and upper(ci.city) = ol.city_nm
        and upper(ci.state) = ol.state_nm
        and upper(ci.countryregionid) = ol.country_nm
        and upper(ci.zipcode) = ol.zip_cd 
        and ol.src_sys_nm = 'AX'
    where ci.dataareaid not in ('044', '045', '047', '999')
),
final as(
    select distinct 
        md5(cast(coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_locn_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_opco_locn_asscn_type_txt as 
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
)) as opco_opco_locn_xref_ck, 
        * 
    from ax_opco_opco_locn_xref 
)

select * from final



    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_opco_locn_xref ) 
    
