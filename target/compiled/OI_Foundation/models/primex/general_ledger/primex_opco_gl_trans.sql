-- depends_on: __dbt__cte__v_primex_opco_curr
-- depends_on: __dbt__cte__v_primex_opco_chart_of_accts_curr
-- depends_on: __dbt__cte__v_primex_opco_gl_trans_type_curr
-- depends_on: __dbt__cte__v_primex_opco_cost_center_curr
-- depends_on: __dbt__cte__v_primex_opco_type_curr
-- depends_on: __dbt__cte__v_primex_opco_dept_curr
-- depends_on: __dbt__cte__v_primex_opco_purpose_curr
-- depends_on: __dbt__cte__v_primex_opco_lob_curr
-- depends_on: __dbt__cte__v_primex_opco_uom_curr
-- depends_on: __dbt__cte__v_primex_opco_sub_ledger_type_curr
-- depends_on: __dbt__cte__v_primex_opco_currency_curr



with  __dbt__cte__v_primex_opco_curr as (
with v_primex_opco as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco
)

select * from v_primex_opco
),  __dbt__cte__v_primex_opco_chart_of_accts_curr as (
with v_primex_opco_chart_of_accts as(
    select *,
    rank() over(partition by opco_chart_of_accts_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_chart_of_accts
),
final as(
    select 
    *
    from v_primex_opco_chart_of_accts
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_primex_opco_gl_trans_type_curr as (
with v_primex_opco_gl_trans_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_gl_trans_type
)

select * from v_primex_opco_gl_trans_type
),  __dbt__cte__v_primex_opco_cost_center_curr as (
with v_primex_opco_cost_center as(
    select *,
    rank() over(partition by opco_cost_center_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_cost_center
),
final as(
    select 
    *
    from v_primex_opco_cost_center
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_primex_opco_type_curr as (
with v_primex_opco_type as(
    select *,
    rank() over(partition by opco_type_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_type
),
final as(
    select 
    *
    from v_primex_opco_type
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_primex_opco_dept_curr as (
with v_primex_opco_dept as(
    select *,
    rank() over(partition by opco_dept_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_dept
),
final as(
    select 
    *
    from v_primex_opco_dept
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_primex_opco_purpose_curr as (
with v_primex_opco_purpose as(
    select *,
    rank() over(partition by opco_purpose_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_purpose
),
final as(
    select 
    *
    from v_primex_opco_purpose
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_primex_opco_lob_curr as (
with v_primex_opco_lob as(
    select *,
    rank() over(partition by opco_lob_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_lob
),
final as(
    select 
    *
    from v_primex_opco_lob
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_primex_opco_uom_curr as (
with v_primex_opco_uom as(
    select *,
    rank() over(partition by opco_uom_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_uom
),
final as(
    select 
    *
    from v_primex_opco_uom
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_primex_opco_sub_ledger_type_curr as (
with v_primex_opco_sub_ledger_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_sub_ledger_type
)

select * from v_primex_opco_sub_ledger_type
),  __dbt__cte__v_primex_opco_currency_curr as (
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
),primex_opco_gl_trans as(
    
    
        select
        'A' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        md5(cast(coalesce(cast('SYSPRO-PRMX' as 
    varchar
), '') || '-' || coalesce(cast(src_comp as 
    varchar
), '') || '-' || coalesce(cast(trim(gjd.glyear) as 
    varchar
), '') || '-' || coalesce(cast(trim(gjd.glperiod) as 
    varchar
), '') || '-' || coalesce(cast(upper(trim(gjd.journal)) as 
    varchar
), '') || '-' || coalesce(cast(trim(gjd.entrynumber) as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_sk, 
        concat_ws('|', 'SYSPRO-PRMX', src_comp, trim(gjd.glyear), trim(gjd.glperiod), upper(trim(gjd.journal)), trim(gjd.entrynumber)) as opco_gl_trans_ak,
        concat_ws('|', src_comp, trim(gjd.glyear), trim(gjd.glperiod), upper(trim(gjd.journal)), trim(gjd.entrynumber)) as src_key_txt,
        iff(gjd.transactiondate is null, gjd.entrydate, gjd.transactiondate) as trans_dt,
        opco.opco_sk,
        ocoa.opco_chart_of_accts_sk,
        ogtt.opco_gl_trans_type_sk,
        null as opco_gl_trans_posting_type_sk,
        occ.opco_cost_center_sk,
        od.opco_dept_sk,
        ot.opco_type_sk,
        op.opco_purpose_sk,
        ol.opco_lob_sk,
        ifnull(oc.opco_currency_sk, (select opco_currency_sk from __dbt__cte__v_primex_opco_curr where src_sys_nm = 'SYSPRO-PRMX' and opco_id = 'A')) as trans_currency_sk,
        null as opco_proj_sk,
        null as original_gl_opco_sk,
        null as original_gl_voucher_nbr,
        null as voucher_nbr,
        null as journal_batch_nbr,
        null as gl_desc,
        null as document_nbr,
        null as document_dt,
        null as crrctn_trans_ind,
        case 
            when gjd.entrytype = 'C' then 1
            when gjd.entrytype = 'D' then 0
        end::numeric(1,0) as credit_ind,
        null as gl_qty,
        case 
            when gjd.entrytype = 'C' then (-1)*gjd.currencyvalue
            else gjd.currencyvalue
        end as trans_currency_gl_amt,
        case 
            when gjd.entrytype = 'C' then (-1)*gjd.entryvalue
            else gjd.entryvalue
        end as opco_currency_gl_amt,
        gjd.glyear as fscl_yr_nbr,
        gjd.glperiod as fscl_mnth_nbr,
        iff(gjd.entryposted = 'Y', 1, 0)::numeric(1,0) as posted_ind,
        ou.opco_uom_sk,
        null as opco_brand_sk,
        oslt.opco_sub_ledger_type_sk,
        null as sub_ledger_cd
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYA_DBO_GENJOURNALDETAIL gjd
        left join MULE_STAGING.SYSPRO_PRIMEX.COMPANYA_DBO_CUSGENMASTER_ cgm
            on trim(gjd.glcode) = trim(cgm.glcode)
        left join MULE_STAGING.SYSPRO_PRIMEX.COMPANYA_DBO_GENMASTER cm
            on trim(gjd.company) = trim(cm.company)
            and trim(gjd.glcode) = trim(cm.glcode)
        left join __dbt__cte__v_primex_opco_curr opco 
            on opco.opco_id = 'A'
            and opco.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_chart_of_accts_curr ocoa 
            on ocoa.opco_id = 'A'
            and ocoa.gl_acct_nbr = upper(trim(gjd.glcode))
            and ocoa.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_gl_trans_type_curr ogtt 
            on ogtt.src_gl_trans_type_cd = upper(trim(gjd.source))
            and ogtt.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_cost_center_curr occ 
            on occ.src_cost_center_cd = upper(trim(cgm.cc))
            and occ.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_dept_curr od 
            on od.src_dept_cd = upper(trim(cgm.dept))
            and od.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_type_curr ot 
            on ot.src_type_cd = upper(trim(cgm.type))
            and ot.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_purpose_curr op
            on op.src_purpose_cd = upper(trim(cgm.purpose))
            and op.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_lob_curr ol
            on ol.src_lob_cd = upper(trim(cgm.fsgroup4buname))
            and ol.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_uom_curr ou 
            on ou.src_uom_cd = upper(trim(cm.glunitofmeasure))
            and ou.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_sub_ledger_type_curr oslt 
            on oslt.src_sub_ledger_type_cd = upper(trim(gjd.type))
            and oslt.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_currency_curr oc 
            on oc.src_currency_cd = upper(trim(gjd.postcurrency))
            and oc.src_sys_nm = 'SYSPRO-PRMX'
         union all 
    
        select
        'C' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        md5(cast(coalesce(cast('SYSPRO-PRMX' as 
    varchar
), '') || '-' || coalesce(cast(src_comp as 
    varchar
), '') || '-' || coalesce(cast(trim(gjd.glyear) as 
    varchar
), '') || '-' || coalesce(cast(trim(gjd.glperiod) as 
    varchar
), '') || '-' || coalesce(cast(upper(trim(gjd.journal)) as 
    varchar
), '') || '-' || coalesce(cast(trim(gjd.entrynumber) as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_sk, 
        concat_ws('|', 'SYSPRO-PRMX', src_comp, trim(gjd.glyear), trim(gjd.glperiod), upper(trim(gjd.journal)), trim(gjd.entrynumber)) as opco_gl_trans_ak,
        concat_ws('|', src_comp, trim(gjd.glyear), trim(gjd.glperiod), upper(trim(gjd.journal)), trim(gjd.entrynumber)) as src_key_txt,
        iff(gjd.transactiondate is null, gjd.entrydate, gjd.transactiondate) as trans_dt,
        opco.opco_sk,
        ocoa.opco_chart_of_accts_sk,
        ogtt.opco_gl_trans_type_sk,
        null as opco_gl_trans_posting_type_sk,
        occ.opco_cost_center_sk,
        od.opco_dept_sk,
        ot.opco_type_sk,
        op.opco_purpose_sk,
        ol.opco_lob_sk,
        ifnull(oc.opco_currency_sk, (select opco_currency_sk from __dbt__cte__v_primex_opco_curr where src_sys_nm = 'SYSPRO-PRMX' and opco_id = 'C')) as trans_currency_sk,
        null as opco_proj_sk,
        null as original_gl_opco_sk,
        null as original_gl_voucher_nbr,
        null as voucher_nbr,
        null as journal_batch_nbr,
        null as gl_desc,
        null as document_nbr,
        null as document_dt,
        null as crrctn_trans_ind,
        case 
            when gjd.entrytype = 'C' then 1
            when gjd.entrytype = 'D' then 0
        end::numeric(1,0) as credit_ind,
        null as gl_qty,
        case 
            when gjd.entrytype = 'C' then (-1)*gjd.currencyvalue
            else gjd.currencyvalue
        end as trans_currency_gl_amt,
        case 
            when gjd.entrytype = 'C' then (-1)*gjd.entryvalue
            else gjd.entryvalue
        end as opco_currency_gl_amt,
        gjd.glyear as fscl_yr_nbr,
        gjd.glperiod as fscl_mnth_nbr,
        iff(gjd.entryposted = 'Y', 1, 0)::numeric(1,0) as posted_ind,
        ou.opco_uom_sk,
        null as opco_brand_sk,
        oslt.opco_sub_ledger_type_sk,
        null as sub_ledger_cd
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYC_DBO_GENJOURNALDETAIL gjd
        left join MULE_STAGING.SYSPRO_PRIMEX.COMPANYC_DBO_CUSGENMASTER_ cgm
            on trim(gjd.glcode) = trim(cgm.glcode)
        left join MULE_STAGING.SYSPRO_PRIMEX.COMPANYC_DBO_GENMASTER cm
            on trim(gjd.company) = trim(cm.company)
            and trim(gjd.glcode) = trim(cm.glcode)
        left join __dbt__cte__v_primex_opco_curr opco 
            on opco.opco_id = 'C'
            and opco.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_chart_of_accts_curr ocoa 
            on ocoa.opco_id = 'C'
            and ocoa.gl_acct_nbr = upper(trim(gjd.glcode))
            and ocoa.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_gl_trans_type_curr ogtt 
            on ogtt.src_gl_trans_type_cd = upper(trim(gjd.source))
            and ogtt.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_cost_center_curr occ 
            on occ.src_cost_center_cd = upper(trim(cgm.cc))
            and occ.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_dept_curr od 
            on od.src_dept_cd = upper(trim(cgm.dept))
            and od.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_type_curr ot 
            on ot.src_type_cd = upper(trim(cgm.type))
            and ot.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_purpose_curr op
            on op.src_purpose_cd = upper(trim(cgm.purpose))
            and op.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_lob_curr ol
            on ol.src_lob_cd = upper(trim(cgm.fsgroup4buname))
            and ol.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_uom_curr ou 
            on ou.src_uom_cd = upper(trim(cm.glunitofmeasure))
            and ou.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_sub_ledger_type_curr oslt 
            on oslt.src_sub_ledger_type_cd = upper(trim(gjd.type))
            and oslt.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_currency_curr oc 
            on oc.src_currency_cd = upper(trim(gjd.postcurrency))
            and oc.src_sys_nm = 'SYSPRO-PRMX'
         union all 
    
        select
        'P' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        md5(cast(coalesce(cast('SYSPRO-PRMX' as 
    varchar
), '') || '-' || coalesce(cast(src_comp as 
    varchar
), '') || '-' || coalesce(cast(trim(gjd.glyear) as 
    varchar
), '') || '-' || coalesce(cast(trim(gjd.glperiod) as 
    varchar
), '') || '-' || coalesce(cast(upper(trim(gjd.journal)) as 
    varchar
), '') || '-' || coalesce(cast(trim(gjd.entrynumber) as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_sk, 
        concat_ws('|', 'SYSPRO-PRMX', src_comp, trim(gjd.glyear), trim(gjd.glperiod), upper(trim(gjd.journal)), trim(gjd.entrynumber)) as opco_gl_trans_ak,
        concat_ws('|', src_comp, trim(gjd.glyear), trim(gjd.glperiod), upper(trim(gjd.journal)), trim(gjd.entrynumber)) as src_key_txt,
        iff(gjd.transactiondate is null, gjd.entrydate, gjd.transactiondate) as trans_dt,
        opco.opco_sk,
        ocoa.opco_chart_of_accts_sk,
        ogtt.opco_gl_trans_type_sk,
        null as opco_gl_trans_posting_type_sk,
        occ.opco_cost_center_sk,
        od.opco_dept_sk,
        ot.opco_type_sk,
        op.opco_purpose_sk,
        ol.opco_lob_sk,
        ifnull(oc.opco_currency_sk, (select opco_currency_sk from __dbt__cte__v_primex_opco_curr where src_sys_nm = 'SYSPRO-PRMX' and opco_id = 'P')) as trans_currency_sk,
        null as opco_proj_sk,
        null as original_gl_opco_sk,
        null as original_gl_voucher_nbr,
        null as voucher_nbr,
        null as journal_batch_nbr,
        null as gl_desc,
        null as document_nbr,
        null as document_dt,
        null as crrctn_trans_ind,
        case 
            when gjd.entrytype = 'C' then 1
            when gjd.entrytype = 'D' then 0
        end::numeric(1,0) as credit_ind,
        null as gl_qty,
        case 
            when gjd.entrytype = 'C' then (-1)*gjd.currencyvalue
            else gjd.currencyvalue
        end as trans_currency_gl_amt,
        case 
            when gjd.entrytype = 'C' then (-1)*gjd.entryvalue
            else gjd.entryvalue
        end as opco_currency_gl_amt,
        gjd.glyear as fscl_yr_nbr,
        gjd.glperiod as fscl_mnth_nbr,
        iff(gjd.entryposted = 'Y', 1, 0)::numeric(1,0) as posted_ind,
        ou.opco_uom_sk,
        null as opco_brand_sk,
        oslt.opco_sub_ledger_type_sk,
        null as sub_ledger_cd
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYP_DBO_GENJOURNALDETAIL gjd
        left join MULE_STAGING.SYSPRO_PRIMEX.COMPANYP_DBO_CUSGENMASTER_ cgm
            on trim(gjd.glcode) = trim(cgm.glcode)
        left join MULE_STAGING.SYSPRO_PRIMEX.COMPANYP_DBO_GENMASTER cm
            on trim(gjd.company) = trim(cm.company)
            and trim(gjd.glcode) = trim(cm.glcode)
        left join __dbt__cte__v_primex_opco_curr opco 
            on opco.opco_id = 'P'
            and opco.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_chart_of_accts_curr ocoa 
            on ocoa.opco_id = 'P'
            and ocoa.gl_acct_nbr = upper(trim(gjd.glcode))
            and ocoa.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_gl_trans_type_curr ogtt 
            on ogtt.src_gl_trans_type_cd = upper(trim(gjd.source))
            and ogtt.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_cost_center_curr occ 
            on occ.src_cost_center_cd = upper(trim(cgm.cc))
            and occ.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_dept_curr od 
            on od.src_dept_cd = upper(trim(cgm.dept))
            and od.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_type_curr ot 
            on ot.src_type_cd = upper(trim(cgm.type))
            and ot.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_purpose_curr op
            on op.src_purpose_cd = upper(trim(cgm.purpose))
            and op.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_lob_curr ol
            on ol.src_lob_cd = upper(trim(cgm.fsgroup4buname))
            and ol.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_uom_curr ou 
            on ou.src_uom_cd = upper(trim(cm.glunitofmeasure))
            and ou.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_sub_ledger_type_curr oslt 
            on oslt.src_sub_ledger_type_cd = upper(trim(gjd.type))
            and oslt.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_currency_curr oc 
            on oc.src_currency_cd = upper(trim(gjd.postcurrency))
            and oc.src_sys_nm = 'SYSPRO-PRMX'
         union all 
    
        select
        'X' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        md5(cast(coalesce(cast('SYSPRO-PRMX' as 
    varchar
), '') || '-' || coalesce(cast(src_comp as 
    varchar
), '') || '-' || coalesce(cast(trim(gjd.glyear) as 
    varchar
), '') || '-' || coalesce(cast(trim(gjd.glperiod) as 
    varchar
), '') || '-' || coalesce(cast(upper(trim(gjd.journal)) as 
    varchar
), '') || '-' || coalesce(cast(trim(gjd.entrynumber) as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_sk, 
        concat_ws('|', 'SYSPRO-PRMX', src_comp, trim(gjd.glyear), trim(gjd.glperiod), upper(trim(gjd.journal)), trim(gjd.entrynumber)) as opco_gl_trans_ak,
        concat_ws('|', src_comp, trim(gjd.glyear), trim(gjd.glperiod), upper(trim(gjd.journal)), trim(gjd.entrynumber)) as src_key_txt,
        iff(gjd.transactiondate is null, gjd.entrydate, gjd.transactiondate) as trans_dt,
        opco.opco_sk,
        ocoa.opco_chart_of_accts_sk,
        ogtt.opco_gl_trans_type_sk,
        null as opco_gl_trans_posting_type_sk,
        occ.opco_cost_center_sk,
        od.opco_dept_sk,
        ot.opco_type_sk,
        op.opco_purpose_sk,
        ol.opco_lob_sk,
        ifnull(oc.opco_currency_sk, (select opco_currency_sk from __dbt__cte__v_primex_opco_curr where src_sys_nm = 'SYSPRO-PRMX' and opco_id = 'X')) as trans_currency_sk,
        null as opco_proj_sk,
        null as original_gl_opco_sk,
        null as original_gl_voucher_nbr,
        null as voucher_nbr,
        null as journal_batch_nbr,
        null as gl_desc,
        null as document_nbr,
        null as document_dt,
        null as crrctn_trans_ind,
        case 
            when gjd.entrytype = 'C' then 1
            when gjd.entrytype = 'D' then 0
        end::numeric(1,0) as credit_ind,
        null as gl_qty,
        case 
            when gjd.entrytype = 'C' then (-1)*gjd.currencyvalue
            else gjd.currencyvalue
        end as trans_currency_gl_amt,
        case 
            when gjd.entrytype = 'C' then (-1)*gjd.entryvalue
            else gjd.entryvalue
        end as opco_currency_gl_amt,
        gjd.glyear as fscl_yr_nbr,
        gjd.glperiod as fscl_mnth_nbr,
        iff(gjd.entryposted = 'Y', 1, 0)::numeric(1,0) as posted_ind,
        ou.opco_uom_sk,
        null as opco_brand_sk,
        oslt.opco_sub_ledger_type_sk,
        null as sub_ledger_cd
        from MULE_STAGING.SYSPRO_PRIMEX.COMPANYX_DBO_GENJOURNALDETAIL gjd
        left join MULE_STAGING.SYSPRO_PRIMEX.COMPANYX_DBO_CUSGENMASTER_ cgm
            on trim(gjd.glcode) = trim(cgm.glcode)
        left join MULE_STAGING.SYSPRO_PRIMEX.COMPANYX_DBO_GENMASTER cm
            on trim(gjd.company) = trim(cm.company)
            and trim(gjd.glcode) = trim(cm.glcode)
        left join __dbt__cte__v_primex_opco_curr opco 
            on opco.opco_id = 'X'
            and opco.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_chart_of_accts_curr ocoa 
            on ocoa.opco_id = 'X'
            and ocoa.gl_acct_nbr = upper(trim(gjd.glcode))
            and ocoa.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_gl_trans_type_curr ogtt 
            on ogtt.src_gl_trans_type_cd = upper(trim(gjd.source))
            and ogtt.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_cost_center_curr occ 
            on occ.src_cost_center_cd = upper(trim(cgm.cc))
            and occ.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_dept_curr od 
            on od.src_dept_cd = upper(trim(cgm.dept))
            and od.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_type_curr ot 
            on ot.src_type_cd = upper(trim(cgm.type))
            and ot.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_purpose_curr op
            on op.src_purpose_cd = upper(trim(cgm.purpose))
            and op.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_lob_curr ol
            on ol.src_lob_cd = upper(trim(cgm.fsgroup4buname))
            and ol.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_uom_curr ou 
            on ou.src_uom_cd = upper(trim(cm.glunitofmeasure))
            and ou.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_sub_ledger_type_curr oslt 
            on oslt.src_sub_ledger_type_cd = upper(trim(gjd.type))
            and oslt.src_sys_nm = 'SYSPRO-PRMX'
        left join __dbt__cte__v_primex_opco_currency_curr oc 
            on oc.src_currency_cd = upper(trim(gjd.postcurrency))
            and oc.src_sys_nm = 'SYSPRO-PRMX'
        
    
),
de_duplication as(
    select *
           , rank() over(partition by opco_gl_trans_ak order by stg_load_dtm, src_comp) as rnk
    from primex_opco_gl_trans
),
final as(
    select  distinct 
            OPCO_GL_TRANS_SK,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') || '-' || coalesce(cast(trans_dt as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_chart_of_accts_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_gl_trans_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_gl_trans_posting_type_sk as 
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
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_proj_sk as 
    varchar
), '') || '-' || coalesce(cast(original_gl_opco_sk as 
    varchar
), '') || '-' || coalesce(cast(original_gl_voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(journal_batch_nbr as 
    varchar
), '') || '-' || coalesce(cast(gl_desc as 
    varchar
), '') || '-' || coalesce(cast(document_nbr as 
    varchar
), '') || '-' || coalesce(cast(document_dt as 
    varchar
), '') || '-' || coalesce(cast(crrctn_trans_ind as 
    varchar
), '') || '-' || coalesce(cast(credit_ind as 
    varchar
), '') || '-' || coalesce(cast(gl_qty as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_gl_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_gl_amt as 
    varchar
), '') || '-' || coalesce(cast(fscl_yr_nbr as 
    varchar
), '') || '-' || coalesce(cast(fscl_mnth_nbr as 
    varchar
), '') || '-' || coalesce(cast(posted_ind as 
    varchar
), '') || '-' || coalesce(cast(opco_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_brand_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sub_ledger_type_sk as 
    varchar
), '') || '-' || coalesce(cast(sub_ledger_cd as 
    varchar
), '') as 
    varchar
)) as OPCO_GL_TRANS_CK,
            OPCO_GL_TRANS_AK, 
            CRT_DTM, STG_LOAD_DTM, DELETE_DTM, SRC_SYS_NM, SRC_KEY_TXT, TRANS_DT, OPCO_SK, OPCO_CHART_OF_ACCTS_SK, OPCO_GL_TRANS_TYPE_SK, OPCO_GL_TRANS_POSTING_TYPE_SK, OPCO_COST_CENTER_SK, OPCO_DEPT_SK, OPCO_TYPE_SK, OPCO_PURPOSE_SK, OPCO_LOB_SK, TRANS_CURRENCY_SK, OPCO_PROJ_SK, ORIGINAL_GL_OPCO_SK, ORIGINAL_GL_VOUCHER_NBR, VOUCHER_NBR, JOURNAL_BATCH_NBR, GL_DESC, DOCUMENT_NBR, DOCUMENT_DT, CRRCTN_TRANS_IND, CREDIT_IND, GL_QTY, TRANS_CURRENCY_GL_AMT, OPCO_CURRENCY_GL_AMT, FSCL_YR_NBR, FSCL_MNTH_NBR, POSTED_IND, OPCO_UOM_SK, OPCO_BRAND_SK, OPCO_SUB_LEDGER_TYPE_SK, SUB_LEDGER_CD
    from de_duplication
    where rnk = 1 
),
delete_captured as(
    select * from final
    
        union
        select
        OPCO_GL_TRANS_sk, OPCO_GL_TRANS_ck,  OPCO_GL_TRANS_AK, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        SRC_SYS_NM, SRC_KEY_TXT, TRANS_DT, OPCO_SK, OPCO_CHART_OF_ACCTS_SK, OPCO_GL_TRANS_TYPE_SK, OPCO_GL_TRANS_POSTING_TYPE_SK, OPCO_COST_CENTER_SK, OPCO_DEPT_SK, OPCO_TYPE_SK, OPCO_PURPOSE_SK, OPCO_LOB_SK, TRANS_CURRENCY_SK, OPCO_PROJ_SK, ORIGINAL_GL_OPCO_SK, ORIGINAL_GL_VOUCHER_NBR, VOUCHER_NBR, JOURNAL_BATCH_NBR, GL_DESC, DOCUMENT_NBR, DOCUMENT_DT, CRRCTN_TRANS_IND, CREDIT_IND, GL_QTY, TRANS_CURRENCY_GL_AMT, OPCO_CURRENCY_GL_AMT, FSCL_YR_NBR, FSCL_MNTH_NBR, POSTED_IND, OPCO_UOM_SK, OPCO_BRAND_SK, OPCO_SUB_LEDGER_TYPE_SK, SUB_LEDGER_CD
        from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_GL_TRANS
        
            where OPCO_GL_TRANS_ck not in (select distinct OPCO_GL_TRANS_ck from final)
         
        and src_sys_nm = 'SYSPRO-PRMX'
    
)

select * from delete_captured



    
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_gl_trans ) and OPCO_GL_TRANS_ck not in (select distinct OPCO_GL_TRANS_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_GL_TRANS where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_gl_trans ) and OPCO_GL_TRANS_ck in (select distinct OPCO_GL_TRANS_ck from OI_DATA_DEV_V2.FND_DEV_BKP.OPCO_GL_TRANS where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    
