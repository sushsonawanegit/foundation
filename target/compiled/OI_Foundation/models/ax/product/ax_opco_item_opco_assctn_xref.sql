


with  __dbt__cte__v_ax_opco_item_curr as (
with v_ax_opco_item as(
    select *,
    rank() over(partition by opco_item_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item
),
final as(
    select 
    *
    from v_ax_opco_item
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_assctn_curr as (
with v_ax_opco_assctn as(
    select *,
    rank() over(partition by opco_assctn_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_assctn
),
final as(
    select 
    *
    from v_ax_opco_assctn
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_item_opco_assctn_xref as(
    select 
    oi.opco_item_sk,
    oa.opco_assctn_sk,
    current_timestamp as crt_dtm,
    iil._fivetran_synced as stg_load_dtm,
    case
        when iil._fivetran_deleted = true then iil._fivetran_synced
        else null
    end as delete_dtm,
    '' as opco_item_count_grp_sk,
    '' as opco_item_scrap_type_sk,
    'AX' as src_sys_nm,
    iil.binlocation_opi as bin_locn_txt,
    iil.buildinglocation_opi as bldg_locn_txt,
    iil.cyds_opi as cubic_yrds_amt,
    iil.cydsfixed_opi as cubic_yrds_fixed_amt,
    iil.cydsvariable_opi as cubic_yrds_variable_amt,
    iil.fixedweight_opi as fixed_wt_amt,
    iil.glprodqty_opi as gl_prodn_qty,
    iil.glsalesqty_opi as gl_sls_qty,
    iil.laborhours_opi as labor_hrs_nbr,
    iil.maxqtyonhand_opi as max_on_hand_qty,
    iil.netweight_opi as net_wt_amt,
    iil.prodabcd_opi as prodn_abcd_cd,
    iil.prodflushingprinciple_opi as prodn_flushing_principle_cd,
    iil.variableweight_opi as variable_wt_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.INVENTITEMLOCATION iil
    left join __dbt__cte__v_ax_opco_item_curr oi 
        on iil.itemid = oi.src_item_cd
        and iil.dataareaid = oi.opco_id
        and oi.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_assctn_curr oa 
        on iil.dataareaid = oa.opco_id
        and iil.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    where iil.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select distinct 
        opco_item_sk, opco_assctn_sk, 
        md5(cast(coalesce(cast(opco_item_count_grp_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_scrap_type_sk as 
    varchar
), '') || '-' || coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(bin_locn_txt as 
    varchar
), '') || '-' || coalesce(cast(bldg_locn_txt as 
    varchar
), '') || '-' || coalesce(cast(cubic_yrds_amt as 
    varchar
), '') || '-' || coalesce(cast(cubic_yrds_fixed_amt as 
    varchar
), '') || '-' || coalesce(cast(cubic_yrds_variable_amt as 
    varchar
), '') || '-' || coalesce(cast(fixed_wt_amt as 
    varchar
), '') || '-' || coalesce(cast(gl_prodn_qty as 
    varchar
), '') || '-' || coalesce(cast(gl_sls_qty as 
    varchar
), '') || '-' || coalesce(cast(labor_hrs_nbr as 
    varchar
), '') || '-' || coalesce(cast(max_on_hand_qty as 
    varchar
), '') || '-' || coalesce(cast(net_wt_amt as 
    varchar
), '') || '-' || coalesce(cast(prodn_abcd_cd as 
    varchar
), '') || '-' || coalesce(cast(prodn_flushing_principle_cd as 
    varchar
), '') || '-' || coalesce(cast(variable_wt_amt as 
    varchar
), '') as 
    varchar
)) as opco_item_opco_assctn_xref_ck,
        crt_dtm, stg_load_dtm, delete_dtm, opco_item_count_grp_sk, opco_item_scrap_type_sk, src_sys_nm, bin_locn_txt, bldg_locn_txt, cubic_yrds_amt, cubic_yrds_fixed_amt, cubic_yrds_variable_amt, fixed_wt_amt, gl_prodn_qty, gl_sls_qty, labor_hrs_nbr, max_on_hand_qty, net_wt_amt, prodn_abcd_cd, prodn_flushing_principle_cd, variable_wt_amt
    from ax_opco_item_opco_assctn_xref 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_opco_assctn_xref ) 
    
