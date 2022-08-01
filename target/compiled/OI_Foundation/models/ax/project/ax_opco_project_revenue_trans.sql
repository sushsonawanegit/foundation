

with  __dbt__cte__v_ax_opco_project_curr as (
with v_ax_opco_project as(
    select *,
    rank() over(partition by opco_project_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project
),
final as(
    select 
    *
    from v_ax_opco_project
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_curr as (
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
),ax_opco_project_revenue_trans as (
    select
    current_timestamp as crt_dtm,
    pro._fivetran_synced as stg_load_dtm,
    case
        when pro._fivetran_deleted = true then pro._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    pro.recid as src_key_txt,
    opr.opco_project_sk,
    opco.opco_sk,
    pro.changeno_opi as chg_nbr,
    pro.projrevenuetype_opi as project_revenue_trans_type_cd,
    pro.comment_opi as project_revenue_trans_desc,
    pro.transdate_opi as trans_dt,
    pro.approved_opi as apprvd_ind,
    pro.projrevenueamount_opi as project_revenue_trans_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.PROJREVENUE_OPI pro
    left join __dbt__cte__v_ax_opco_project_curr opr
        on pro.dataareaid = opr.opco_id
        and pro.projid_opi = opr.project_id
        and opr.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_curr opco
        on pro.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    where pro.dataareaid not in ('044', '045', '047', '999')
),
final as (															
    select 
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') as 
    varchar
)) as opco_project_revenue_trans_sk,
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') || '-' || coalesce(cast(opco_project_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(chg_nbr as 
    varchar
), '') || '-' || coalesce(cast(project_revenue_trans_type_cd as 
    varchar
), '') || '-' || coalesce(cast(project_revenue_trans_desc as 
    varchar
), '') || '-' || coalesce(cast(trans_dt as 
    varchar
), '') || '-' || coalesce(cast(apprvd_ind as 
    varchar
), '') || '-' || coalesce(cast(project_revenue_trans_amt as 
    varchar
), '') as 
    varchar
)) as opco_project_revenue_trans_ck,
        concat_ws('|', src_sys_nm, src_key_txt) as opco_project_revenue_trans_ak,
        * 		
    from ax_opco_project_revenue_trans 															
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_revenue_trans ) 
    
