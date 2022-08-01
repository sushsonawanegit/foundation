

with  __dbt__cte__v_ns_opco_cost_center_curr as (
with v_ns_opco_cost_center as(
    select *,
    rank() over(partition by opco_cost_center_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_cost_center
),
final as(
    select 
    *
    from v_ns_opco_cost_center
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ns_opco_dept_curr as (
with v_ns_opco_dept as(
    select *,
    rank() over(partition by opco_dept_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_dept
),
final as(
    select 
    *
    from v_ns_opco_dept
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ns_opco_type_curr as (
with v_ns_opco_type as(
    select *,
    rank() over(partition by opco_type_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_type
),
final as(
    select 
    *
    from v_ns_opco_type
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ns_opco_chart_of_accts_type_curr as (
with v_ns_opco_chart_of_accts_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_chart_of_accts_type
)

select * from v_ns_opco_chart_of_accts_type
),  __dbt__cte__v_ns_opco_uom_curr as (
with v_ns_opco_uom as(
    select *,
    rank() over(partition by opco_uom_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_uom
),
final as(
    select 
    *
    from v_ns_opco_uom
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ns_opco_brand_curr as (
with v_ns_opco_brand as(
    select *,
    rank() over(partition by opco_brand_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_brand
),
final as(
    select 
    *
    from v_ns_opco_brand
    where rnk = 1 and delete_dtm is null
)

select * from final
),ns_opco_chart_of_accts as (
    select 
    current_timestamp as crt_dtm,
    acct._fivetran_synced as stg_load_dtm,
    case
        when acct._fivetran_deleted = true then acct._fivetran_synced
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    acct.accountnumber::varchar(20) as gl_acct_nbr,
    null::varchar(15) as opco_id,
    acct.name::varchar(100) as gl_acct_nm,
    coa.chart_of_accts_sk,
    cc.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    null::varchar(32) as opco_purpose_sk,
    null::varchar(32) as opco_lob_sk,
    cat.opco_chart_of_accts_type_sk,
    ou.opco_uom_sk,
    acct.full_name::varchar(120) as gl_acct_alias_nm,
    null::number(1,0) as manual_entry_allowed_ind,
    case 
        when acct.dpo_flag = 'T' then 0::number(1,0)
        when acct.dpo_flag = 'F' then 1::number(1,0)
    end as dpo_exclusion_ind,
    null::number(1,0) as movement_allowed_ind,
    null::varchar(50) as cost_center_reqd_txt,
    null::varchar(50) as dept_reqd_txt,
    null::varchar(50) as type_reqd_txt,
    acct.parent_id::varchar(10) as prnt_gl_acct_nbr,
    null::varchar(10) as related_gl_acct_nbr,
    null::varchar(10) as monetary_gl_acct_ind,
    iff(acct.isinactive='No', 1::number(1,0), 0::number(1,0)) as actv_ind,
    case
        when is_balancesheet = 'T' then 1::number(1,0)
        when is_balancesheet = 'F' then 0::number(1,0)
    end as balance_sheet_acct_ind,
    case
        when is_summary = 'Yes' then 1::number(1,0)
        when is_summary = 'No' then 0::number(1,0)
    end as summary_acct_ind,
    ob.opco_brand_sk,
    null::varchar(20) as acct_clssfctn_cd,
    null::varchar(20) as sub_acct_nbr
    from FIVETRAN.NETSUITE.ACCOUNTS acct
    left join OI_DATA_DEV_V2.FND_DEV_BKP.chart_of_accts coa 
        on left(acct.opi_ax_acct, 6) = coa.gl_acct_nbr
        and acct.opi_ax_acct is not null
        and acct.opi_ax_acct not in ('none', 'None')
    left join __dbt__cte__v_ns_opco_cost_center_curr cc 
        on upper(acct.location_id) = cc.src_cost_center_cd
        and cc.src_sys_nm = 'NS'
    left join __dbt__cte__v_ns_opco_dept_curr od 
        on upper(acct.department_id) = od.src_dept_cd
        and od.src_sys_nm = 'NS'
    left join __dbt__cte__v_ns_opco_type_curr ot 
        on upper(substr(acct.opi_ax_acct, 8)) = ot.src_type_cd
        and ot.src_sys_nm = 'NS'
        and acct.opi_ax_acct is not null
    left join __dbt__cte__v_ns_opco_chart_of_accts_type_curr cat 
        on upper(acct.type_name) = cat.src_acct_type_cd
        and cat.src_sys_nm = 'NS'
    left join __dbt__cte__v_ns_opco_uom_curr ou 
        on upper(acct.opi_qty_uom_id) = ou.src_uom_cd
        and ou.src_sys_nm = 'NS'
    left join __dbt__cte__v_ns_opco_brand_curr ob 
        on acct.class_id = ob.src_brand_cd
        and ob.src_sys_nm = 'NS'
    where acct.accountnumber is not null
),
final as (
    select  md5(cast(coalesce(cast(cat.src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(cat.gl_acct_nbr as 
    varchar
), '') || '-' || coalesce(cast(sb.subsidiary_id as 
    varchar
), '') as 
    varchar
)) as opco_chart_of_accts_sk,
            md5(cast(coalesce(cast(cat.src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(cat.gl_acct_nbr as 
    varchar
), '') || '-' || coalesce(cast(sb.subsidiary_id as 
    varchar
), '') || '-' || coalesce(cast(cat.gl_acct_nm as 
    varchar
), '') || '-' || coalesce(cast(cat.chart_of_accts_sk as 
    varchar
), '') || '-' || coalesce(cast(cat.opco_cost_center_sk as 
    varchar
), '') || '-' || coalesce(cast(cat.opco_dept_sk as 
    varchar
), '') || '-' || coalesce(cast(cat.opco_type_sk as 
    varchar
), '') || '-' || coalesce(cast(cat.opco_purpose_sk as 
    varchar
), '') || '-' || coalesce(cast(cat.opco_lob_sk as 
    varchar
), '') || '-' || coalesce(cast(cat.opco_chart_of_accts_type_sk as 
    varchar
), '') || '-' || coalesce(cast(cat.opco_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(cat.gl_acct_alias_nm as 
    varchar
), '') || '-' || coalesce(cast(cat.manual_entry_allowed_ind as 
    varchar
), '') || '-' || coalesce(cast(cat.dpo_exclusion_ind as 
    varchar
), '') || '-' || coalesce(cast(cat.movement_allowed_ind as 
    varchar
), '') || '-' || coalesce(cast(cat.cost_center_reqd_txt as 
    varchar
), '') || '-' || coalesce(cast(cat.dept_reqd_txt as 
    varchar
), '') || '-' || coalesce(cast(cat.type_reqd_txt as 
    varchar
), '') || '-' || coalesce(cast(accts.accountnumber as 
    varchar
), '') || '-' || coalesce(cast(cat.related_gl_acct_nbr as 
    varchar
), '') || '-' || coalesce(cast(cat.monetary_gl_acct_ind as 
    varchar
), '') || '-' || coalesce(cast(cat.actv_ind as 
    varchar
), '') || '-' || coalesce(cast(cat.balance_sheet_acct_ind as 
    varchar
), '') || '-' || coalesce(cast(cat.summary_acct_ind as 
    varchar
), '') || '-' || coalesce(cast(cat.opco_brand_sk as 
    varchar
), '') || '-' || coalesce(cast(cat.acct_clssfctn_cd as 
    varchar
), '') || '-' || coalesce(cast(cat.sub_acct_nbr as 
    varchar
), '') as 
    varchar
)) as opco_chart_of_accts_ck,
            cat.crt_dtm, cat.stg_load_dtm, cat.delete_dtm, cat.src_sys_nm, cat.gl_acct_nbr, sb.subsidiary_id as opco_id, cat.gl_acct_nm, cat.chart_of_accts_sk, cat.opco_cost_center_sk, cat.opco_dept_sk, cat.opco_type_sk, cat.opco_purpose_sk, cat.opco_lob_sk, cat.opco_chart_of_accts_type_sk, cat.opco_uom_sk, cat.gl_acct_alias_nm, cat.manual_entry_allowed_ind, cat.dpo_exclusion_ind, cat.movement_allowed_ind, cat.cost_center_reqd_txt, cat.dept_reqd_txt, cat.type_reqd_txt, accts.accountnumber as prnt_gl_acct_nbr, cat.related_gl_acct_nbr, cat.monetary_gl_acct_ind, cat.actv_ind, cat.balance_sheet_acct_ind, cat.summary_acct_ind, cat.opco_brand_sk, cat.acct_clssfctn_cd, cat.sub_acct_nbr
    from ns_opco_chart_of_accts cat
    left join FIVETRAN.NETSUITE.ACCOUNTS accts 
        on accts.account_id = cat.prnt_gl_acct_nbr
    cross join FIVETRAN.NETSUITE.SUBSIDIARIES sb 
        where sb.subsidiary_id not in (1, 4, 5, 7)
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_chart_of_accts ) 
    
