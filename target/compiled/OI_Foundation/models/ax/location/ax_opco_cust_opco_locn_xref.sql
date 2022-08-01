

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
),  __dbt__cte__v_ax_opco_cust_curr as (
with v_ax_opco_cust as(
    select *,
    rank() over(partition by opco_cust_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust
),
final as(
    select 
    *
    from v_ax_opco_cust
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_cust_opco_locn_xref as(
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
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CONTACTPERSON cp
    left join FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTTABLE ct
        on cp.contactpersonid = ct.contactpersonid
        and cp.dataareaid = ct.dataareaid
        and cp.dataareaid not in ('044', '045', '047', '999')
        and ct.dataareaid not in ('044', '045', '047', '999')
        and ct._fivetran_deleted = false
    left join __dbt__cte__v_ax_opco_locn_curr ol
        on upper(ct.street) = ol.addr_ln_1_txt
        and upper(ct.city) = ol.city_nm
        and upper(ct.state) = ol.state_nm
        and upper(ct.countryregionid) = ol.country_nm
        and upper(ct.zipcode) = ol.zip_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr oc 
        on ct.accountnum = oc.cust_id
        and ct.dataareaid = oc.opco_id
        and oc.src_sys_nm = 'AX'
),
final as (
    select distinct 
            md5(cast(coalesce(cast(opco_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_locn_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_opco_locn_asscn_type_txt as 
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
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') as 
    varchar
)) as opco_cust_opco_locn_xref_ck,
            * 
    from ax_opco_cust_opco_locn_xref
)

select * from final



    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_opco_locn_xref ) 
    
