

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
),ax_sls_ordr_opco_locn_xref as (
    select 
    md5(cast(coalesce(cast('AX' as 
    varchar
), '') || '-' || coalesce(cast(st.dataareaid as 
    varchar
), '') || '-' || coalesce(cast(st.salesid as 
    varchar
), '') as 
    varchar
)) as opco_sls_ordr_sk,
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
    from FIVETRAN.AX_PRODREPLICATION1_DBO.SALESTABLE st
    left join FIVETRAN.AX_PRODREPLICATION1_DBO.CONTACTPERSON cp  
        on cp.dataareaid = st.dataareaid
        and cp.contactpersonid = st.contactpersonid
        and cp.dataareaid not in ('044', '045', '047', '999')
        and st.dataareaid not in ('044', '045', '047', '999')
        and cp._fivetran_deleted = false
    left join __dbt__cte__v_ax_opco_locn_curr ol 
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
        md5(cast(coalesce(cast(opco_sls_ordr_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_locn_sk as 
    varchar
), '') || '-' || coalesce(cast(sls_ordr_opco_locn_asscn_type_txt as 
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
), '') || '-' || coalesce(cast(languate_txt as 
    varchar
), '') || '-' || coalesce(cast(start_dt as 
    varchar
), '') || '-' || coalesce(cast(end_dt as 
    varchar
), '') as 
    varchar
)) as sls_ordr_opco_locn_xref_ck,
        crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, contact_type_txt, contact_nm, contact_ph_nbr, contact_ph_extn_nbr, contact_cell_nbr, contact_local_ph_nbr, contact_fax_nbr, contact_telex_nbr, contact_email_id, languate_txt, start_dt, end_dt
    from ax_sls_ordr_opco_locn_xref 
)

select * from final   




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_sls_ordr_opco_locn_xref ) 
    
