

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco  as
      (

with  __dbt__cte__v_primex_opco_currency_curr as (
with v_primex_opco_currency as(
    select *,
    rank() over(partition by opco_currency_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_currency
),
final as(
    select 
    *
    from v_primex_opco_currency
    where rnk = 1 and delete_dtm is null
)

select * from final
),primex_opco as(
    
        select 
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        concat_ws('|', 'SYSPRO-PRMX', 'A') as opco_ak,
        'SYSPRO-PRMX' as src_sys_nm,
        'A' as opco_id,
        case 
            when 'A' = 'A' then 'Primex Manufacturing Ltd.'
            when 'A' = 'C' then 'Primex Manufacturing Corp'
            when 'A' = 'P' then 'Primex Technologies Inc.'
            when 'A' = 'X' then 'Primex Elimination Co.'
        end as opco_nm,
        (select upper(trim(currency)) as currency_cd from MULE_STAGING.SYSPRO_PRIMEX.COMPANYA_DBO_TBLCURRENCY where buyexchangerate = 1) as opco_currency_key,
        case 
            when 'A' = 'A' then '2019-07-07'::date
            when 'A' = 'C' then '2020-03-11'::date
            when 'A' = 'X' then '2019-07-07'::date
            else null
        end as acqstn_dt,
        iff('A' in ('A', 'C', 'X'), 1, 0)::number(1,0) as actv_ind,
        case 
            when 'A' = 'P' then '2022-01-01'::date
            else null
        end as inactv_dt
         union all 
    
        select 
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        concat_ws('|', 'SYSPRO-PRMX', 'C') as opco_ak,
        'SYSPRO-PRMX' as src_sys_nm,
        'C' as opco_id,
        case 
            when 'C' = 'A' then 'Primex Manufacturing Ltd.'
            when 'C' = 'C' then 'Primex Manufacturing Corp'
            when 'C' = 'P' then 'Primex Technologies Inc.'
            when 'C' = 'X' then 'Primex Elimination Co.'
        end as opco_nm,
        (select upper(trim(currency)) as currency_cd from MULE_STAGING.SYSPRO_PRIMEX.COMPANYC_DBO_TBLCURRENCY where buyexchangerate = 1) as opco_currency_key,
        case 
            when 'C' = 'A' then '2019-07-07'::date
            when 'C' = 'C' then '2020-03-11'::date
            when 'C' = 'X' then '2019-07-07'::date
            else null
        end as acqstn_dt,
        iff('C' in ('A', 'C', 'X'), 1, 0)::number(1,0) as actv_ind,
        case 
            when 'C' = 'P' then '2022-01-01'::date
            else null
        end as inactv_dt
         union all 
    
        select 
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        concat_ws('|', 'SYSPRO-PRMX', 'P') as opco_ak,
        'SYSPRO-PRMX' as src_sys_nm,
        'P' as opco_id,
        case 
            when 'P' = 'A' then 'Primex Manufacturing Ltd.'
            when 'P' = 'C' then 'Primex Manufacturing Corp'
            when 'P' = 'P' then 'Primex Technologies Inc.'
            when 'P' = 'X' then 'Primex Elimination Co.'
        end as opco_nm,
        (select upper(trim(currency)) as currency_cd from MULE_STAGING.SYSPRO_PRIMEX.COMPANYP_DBO_TBLCURRENCY where buyexchangerate = 1) as opco_currency_key,
        case 
            when 'P' = 'A' then '2019-07-07'::date
            when 'P' = 'C' then '2020-03-11'::date
            when 'P' = 'X' then '2019-07-07'::date
            else null
        end as acqstn_dt,
        iff('P' in ('A', 'C', 'X'), 1, 0)::number(1,0) as actv_ind,
        case 
            when 'P' = 'P' then '2022-01-01'::date
            else null
        end as inactv_dt
         union all 
    
        select 
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        concat_ws('|', 'SYSPRO-PRMX', 'X') as opco_ak,
        'SYSPRO-PRMX' as src_sys_nm,
        'X' as opco_id,
        case 
            when 'X' = 'A' then 'Primex Manufacturing Ltd.'
            when 'X' = 'C' then 'Primex Manufacturing Corp'
            when 'X' = 'P' then 'Primex Technologies Inc.'
            when 'X' = 'X' then 'Primex Elimination Co.'
        end as opco_nm,
        (select upper(trim(currency)) as currency_cd from MULE_STAGING.SYSPRO_PRIMEX.COMPANYX_DBO_TBLCURRENCY where buyexchangerate = 1) as opco_currency_key,
        case 
            when 'X' = 'A' then '2019-07-07'::date
            when 'X' = 'C' then '2020-03-11'::date
            when 'X' = 'X' then '2019-07-07'::date
            else null
        end as acqstn_dt,
        iff('X' in ('A', 'C', 'X'), 1, 0)::number(1,0) as actv_ind,
        case 
            when 'X' = 'P' then '2022-01-01'::date
            else null
        end as inactv_dt
        
    
),
final as(
    select  md5(cast(coalesce(cast(o.src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') as 
    varchar
)) as opco_sk,
            md5(cast(coalesce(cast(o.src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(opco_nm as 
    varchar
), '') || '-' || coalesce(cast(acqstn_dt as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') || '-' || coalesce(cast(inactv_dt as 
    varchar
), '') as 
    varchar
)) as opco_ck,
            o.opco_ak, o.crt_dtm, o.stg_load_dtm, o.delete_dtm, o.src_sys_nm, o.opco_id, o.opco_nm, o.acqstn_dt, o.actv_ind, o.inactv_dt, oc.opco_currency_sk
    from primex_opco o
    left join __dbt__cte__v_primex_opco_currency_curr oc 
        on oc.src_currency_cd = opco_currency_key
        and oc.src_sys_nm = 'SYSPRO-PRMX'
)

select * from final
      );
    