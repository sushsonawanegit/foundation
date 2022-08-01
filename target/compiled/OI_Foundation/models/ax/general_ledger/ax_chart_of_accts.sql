

with  __dbt__cte__v_ax_opco_cost_center_curr as (
with v_ax_opco_cost_center as(
    select *,
    rank() over(partition by opco_cost_center_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cost_center
),
final as(
    select 
    *
    from v_ax_opco_cost_center
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_dept_curr as (
with v_ax_opco_dept as(
    select *,
    rank() over(partition by opco_dept_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dept
),
final as(
    select 
    *
    from v_ax_opco_dept
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_type_curr as (
with v_ax_opco_type as(
    select *,
    rank() over(partition by opco_type_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_type
),
final as(
    select 
    *
    from v_ax_opco_type
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_purpose_curr as (
with v_ax_opco_purpose as(
    select *,
    rank() over(partition by opco_purpose_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_purpose
),
final as(
    select 
    *
    from v_ax_opco_purpose
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_lob_curr as (
with v_ax_opco_lob as(
    select *,
    rank() over(partition by opco_lob_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_lob
),
final as(
    select 
    *
    from v_ax_opco_lob
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_chart_of_accts_type_curr as (
with v_ax_opco_chart_of_accts_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_chart_of_accts_type
)

select * from v_ax_opco_chart_of_accts_type
),  __dbt__cte__v_ax_opco_uom_curr as (
with v_ax_opco_uom as(
    select *,
    rank() over(partition by opco_uom_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_uom
),
final as(
    select 
    *
    from v_ax_opco_uom
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_chart_of_accts as(
    select 
    current_timestamp as crt_dtm,
    lt._fivetran_synced as stg_load_dtm,
    case
        when lt._fivetran_deleted = true then lt._fivetran_synced
        else null
    end as delete_dtm,
    lt.accountnum as gl_acct_nbr,
    lt.accountname as gl_acct_nm,
    cc.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    oip.opco_purpose_sk,
    ol.opco_lob_sk,
    cat.opco_chart_of_accts_type_sk,
    ou.opco_uom_sk,
    lt.accountnamealias as gl_acct_alias_nm,
    lt.blockedinjournal as blocked_in_journal_ind,
    lt.excludefromdpo_opi as dpo_exclusion_ind,
    lt.movementjournal_opi as movement_allowed_ind,
    case 
        when lt.mandatorydimension = 0 then 'Optional'
        when lt.mandatorydimension = 1 then 'To Be Filled In'
        when lt.mandatorydimension = 2 then 'Table'
        when lt.mandatorydimension = 3 then 'List'
        when lt.mandatorydimension = 4 then 'Fixed'
        when lt.mandatorydimension = 5 then 'Default'
    end as cost_center_reqd_txt,
    case 
        when lt.mandatorydimension2_ = 0 then 'Optional'
        when lt.mandatorydimension2_ = 1 then 'To Be Filled In'
        when lt.mandatorydimension2_ = 2 then 'Table'
        when lt.mandatorydimension2_ = 3 then 'List'
        when lt.mandatorydimension2_ = 4 then 'Fixed'
        when lt.mandatorydimension2_ = 5 then 'Default'
    end as dept_reqd_txt,
    case 
        when lt.mandatorydimension3_ = 0 then 'Optional'
        when lt.mandatorydimension3_ = 1 then 'To Be Filled In'
        when lt.mandatorydimension3_ = 2 then 'Table'
        when lt.mandatorydimension3_ = 3 then 'List'
        when lt.mandatorydimension3_ = 4 then 'Fixed'
        when lt.mandatorydimension3_ = 5 then 'Default'
    end as type_reqd_txt,
    lt.parentaccount_opi as prnt_gl_acct_nbr,
    lt.relatedaccount_opi as related_gl_acct_nbr,
    lt.monetary as monetary_gl_acct_ind,
    iff(lt.closed=0, 1,0) as actv_ind,
    case 
        when lt.accountpltype = 3 then 1
        else 0
    end as balance_sheet_acct_ind,
    case 
        when lt.accountpltype = 9 then 1
        else 0
    end as summary_acct_ind
    from FIVETRAN.AX_PRODREPLICATION1_DBO.LEDGERTABLE lt
    left join __dbt__cte__v_ax_opco_cost_center_curr cc 
        on upper(lt.dimension) = cc.src_cost_center_cd
        and cc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(lt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(lt.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr oip
        on upper(lt.dimension4_) = oip.src_purpose_cd
        and oip.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol 
        on upper(lt.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_chart_of_accts_type_curr cat 
        on cast(lt.accountpltype as varchar) = cat.src_acct_type_cd
    left join __dbt__cte__v_ax_opco_uom_curr ou 
        on upper(lt.unitid_opi) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    where upper(lt.dataareaid) = 'UGL' 
    and lt.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select  md5(cast(coalesce(cast(gl_acct_nbr as 
    varchar
), '') as 
    varchar
)) as chart_of_accts_sk, 
            md5(cast(coalesce(cast(gl_acct_nbr as 
    varchar
), '') || '-' || coalesce(cast(gl_acct_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_cost_center_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dept_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_purpose_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_lob_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_chart_of_accts_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(gl_acct_alias_nm as 
    varchar
), '') || '-' || coalesce(cast(blocked_in_journal_ind as 
    varchar
), '') || '-' || coalesce(cast(dpo_exclusion_ind as 
    varchar
), '') || '-' || coalesce(cast(movement_allowed_ind as 
    varchar
), '') || '-' || coalesce(cast(cost_center_reqd_txt as 
    varchar
), '') || '-' || coalesce(cast(dept_reqd_txt as 
    varchar
), '') || '-' || coalesce(cast(type_reqd_txt as 
    varchar
), '') || '-' || coalesce(cast(prnt_gl_acct_nbr as 
    varchar
), '') || '-' || coalesce(cast(related_gl_acct_nbr as 
    varchar
), '') || '-' || coalesce(cast(monetary_gl_acct_ind as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') || '-' || coalesce(cast(balance_sheet_acct_ind as 
    varchar
), '') || '-' || coalesce(cast(summary_acct_ind as 
    varchar
), '') as 
    varchar
)) as chart_of_accts_ck,
            * 
    from ax_chart_of_accts 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_chart_of_accts ) 
    
