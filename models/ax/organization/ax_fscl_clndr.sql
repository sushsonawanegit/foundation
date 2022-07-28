{% set _load = load_type('AX_FSCL_CLNDR') %}

with ax_fscl_calendar as (
    select 
    {{ dbt_utils.surrogate_key(['date(date_key)']) }} as fscl_clndr_sk,
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case 
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    1 as busn_apprvd_ind,
    date(date_key) as clndr_dt,
    date_altkey as dt_ak,
    serial_month as serial_mnth_nbr,
    year_id as fscl_yr_id,
    year_name as fscl_yr_nm,
    month_id as fscl_yr_mnth_id,
    month_name as fscl_yr_mnth_nm,
    month_no as fscl_mnth_nbr,
    month_mmm as fscl_mnth_nm,
    cntous_rate as cn_to_us_cnvrsn_rt,
    date(begin_day) as fscl_mnth_strt_dt,
    date(end_date) as fscl_mnth_end_dt,
    date(prev_begin) as prev_fscl_mnth_strt_dt,
    date(prev_end) as prev_fscl_mnth_end_dt,
    date(year_begin) as fscl_yr_strt_dt,
    date(year_end) as fscl_yr_end_dt,
    days as fscl_mnth_days_nbr,
    period_id as fscl_period_id,
    day_of_week as fscl_wk_day_nbr,
    is_week_day as wk_day_ind,
    calendar_year as clndr_yr_nbr,
    calendar_day_of_year as clndr_yr_day_nbr,
    calendar_quarter as clndr_qtr_nbr,
    calendar_quarter_of_year as clndr_yr_qtr_nbr,
    calendar_day_of_quarter as clndr_qtr_day_nbr,
    calendar_month_of_quarter as clndr_qtr_mnth_nbr,
    calendar_month as clndr_mnth_nbr,
    calendar_month_of_year as clndr_yr_mnth_nbr,
    calendar_day_of_month as clndr_mnth_day_nbr,
    calendar_week_year as clndr_wk_yr_nbr,
    calendar_week_day_of_year as clndr_yr_wk_day_nbr,
    calendar_week_week as clndr_wk_nbr,
    calendar_week_week_of_year as clndr_yr_wk_nbr,
    fiscal_year as fscl_yr_nbr,
    fiscal_day_of_year as fscl_yr_day_nbr,
    fiscal_quarter_of_year as fscl_qtr_nbr,
    fiscal_quarter as fscl_yr_qtr_nbr,
    fiscal_day_of_quarter as fscl_qtr_day_nbr,
    fiscal_month_of_quarter as fscl_qtr_mnth_nbr,
    fiscal_month as fscl_yr_mnth_nbr,
    fiscal_month_of_year as fscl_mnth_nbr_1,
    fiscal_day_of_month as fscl_mnth_day_nbr,
    fiscal_week_year as fscl_wk_yr_nbr,
    fiscal_week_day_of_year as fscl_yr_wk_day_nbr,
    fiscal_week_week as fscl_yr_wk_nbr,
    fiscal_week_week_of_year as fscl_wk_nbr,
    datevalue_caption_1033 as dt_caption_1033_txt,
    day_of_week_caption_1033 as wk_day_caption_1033_txt,
    calendar_year_caption_1033 as clndr_yr_caption_1033_txt,
    calendar_quarter_caption_1033 as clndr_qtr_caption_1033_txt,
    calendar_quarter_of_year_caption_1033 as clndr_yr_qtr_caption_1033_txt,
    calendar_month_caption_1033 as clndr_mnth_caption_1033_txt,
    calendar_month_of_year_caption_1033 as clndr_yr_mnth_caption_1033_txt,
    calendar_week_year_caption_1033 as clndr_wk_yr_caption_1033_txt,
    calendar_week_week_caption_1033 as clndr_wk_caption_1033_txt,
    calendar_week_week_of_year_caption_1033 as clndr_yr_wk_caption_1033_txt,
    fiscal_year_caption_1033 as fscl_yr_caption_1033_txt,
    fiscal_quarter_of_year_caption_1033 as fscl_qtr_caption_1033_txt,
    fiscal_quarter_caption_1033 as fscl_yr_qtr_caption_1033_txt,
    fiscal_month_of_year_caption_1033 as fscl_mnth_caption_1033_txt,
    fiscal_month_caption_1033 as fscl_yr_mnth_caption_1033_txt,
    fiscal_week_year_caption_1033 as fscl_wk_yr_caption_1033_txt,
    fiscal_week_week_caption_1033 as fscl_yr_wk_caption_1033_txt,
    fiscal_week_week_of_year_caption_1033 as fscl_wk_caption_1033_txt
    from {{ source('AX_PRODVIEW', 'DIM_TIME_93')}}
),
final as(
    select  fscl_clndr_sk,
            {{ dbt_utils.surrogate_key(['busn_apprvd_ind', 'clndr_dt', 'dt_ak', 'serial_mnth_nbr', 'fscl_yr_id', 'fscl_yr_nm', 'fscl_yr_mnth_id', 'fscl_yr_mnth_nm', 'fscl_mnth_nbr', 'fscl_mnth_nm', 'cn_to_us_cnvrsn_rt', 'fscl_mnth_strt_dt', 'fscl_mnth_end_dt', 'prev_fscl_mnth_strt_dt', 'prev_fscl_mnth_end_dt', 'fscl_yr_strt_dt', 'fscl_yr_end_dt', 'fscl_mnth_days_nbr', 'fscl_period_id', 'fscl_wk_day_nbr', 'wk_day_ind', 'clndr_yr_nbr', 'clndr_yr_day_nbr', 'clndr_qtr_nbr', 'clndr_yr_qtr_nbr', 'clndr_qtr_day_nbr', 'clndr_qtr_mnth_nbr', 'clndr_mnth_nbr', 'clndr_yr_mnth_nbr', 'clndr_mnth_day_nbr', 'clndr_wk_yr_nbr', 'clndr_yr_wk_day_nbr', 'clndr_wk_nbr', 'clndr_yr_wk_nbr', 'fscl_yr_nbr', 'fscl_yr_day_nbr', 'fscl_qtr_nbr', 'fscl_yr_qtr_nbr', 'fscl_qtr_day_nbr', 'fscl_qtr_mnth_nbr', 'fscl_yr_mnth_nbr', 'fscl_mnth_nbr_1', 'fscl_mnth_day_nbr', 'fscl_wk_yr_nbr', 'fscl_yr_wk_day_nbr', 'fscl_yr_wk_nbr', 'fscl_wk_nbr', 'dt_caption_1033_txt', 'wk_day_caption_1033_txt', 'clndr_yr_caption_1033_txt', 'clndr_qtr_caption_1033_txt', 'clndr_yr_qtr_caption_1033_txt', 'clndr_mnth_caption_1033_txt', 'clndr_yr_mnth_caption_1033_txt', 'clndr_wk_yr_caption_1033_txt', 'clndr_wk_caption_1033_txt', 'clndr_yr_wk_caption_1033_txt', 'fscl_yr_caption_1033_txt', 'fscl_qtr_caption_1033_txt', 'fscl_yr_qtr_caption_1033_txt', 'fscl_mnth_caption_1033_txt', 'fscl_yr_mnth_caption_1033_txt', 'fscl_wk_yr_caption_1033_txt', 'fscl_yr_wk_caption_1033_txt', 'fscl_wk_caption_1033_txt']) }} as fscl_clndr_ck,
            crt_dtm, stg_load_dtm, delete_dtm, busn_apprvd_ind, clndr_dt, dt_ak, serial_mnth_nbr, fscl_yr_id, fscl_yr_nm, fscl_yr_mnth_id, fscl_yr_mnth_nm, fscl_mnth_nbr, fscl_mnth_nm, cn_to_us_cnvrsn_rt, fscl_mnth_strt_dt, fscl_mnth_end_dt, prev_fscl_mnth_strt_dt, prev_fscl_mnth_end_dt, fscl_yr_strt_dt, fscl_yr_end_dt, fscl_mnth_days_nbr, fscl_period_id, fscl_wk_day_nbr, wk_day_ind, clndr_yr_nbr, clndr_yr_day_nbr, clndr_qtr_nbr, clndr_yr_qtr_nbr, clndr_qtr_day_nbr, clndr_qtr_mnth_nbr, clndr_mnth_nbr, clndr_yr_mnth_nbr, clndr_mnth_day_nbr, clndr_wk_yr_nbr, clndr_yr_wk_day_nbr, clndr_wk_nbr, clndr_yr_wk_nbr, fscl_yr_nbr, fscl_yr_day_nbr, fscl_qtr_nbr, fscl_yr_qtr_nbr, fscl_qtr_day_nbr, fscl_qtr_mnth_nbr, fscl_yr_mnth_nbr, fscl_mnth_nbr_1, fscl_mnth_day_nbr, fscl_wk_yr_nbr, fscl_yr_wk_day_nbr, fscl_yr_wk_nbr, fscl_wk_nbr, dt_caption_1033_txt, wk_day_caption_1033_txt, clndr_yr_caption_1033_txt, clndr_qtr_caption_1033_txt, clndr_yr_qtr_caption_1033_txt, clndr_mnth_caption_1033_txt, clndr_yr_mnth_caption_1033_txt, clndr_wk_yr_caption_1033_txt, clndr_wk_caption_1033_txt, clndr_yr_wk_caption_1033_txt, fscl_yr_caption_1033_txt, fscl_qtr_caption_1033_txt, fscl_yr_qtr_caption_1033_txt, fscl_mnth_caption_1033_txt, fscl_yr_mnth_caption_1033_txt, fscl_wk_yr_caption_1033_txt, fscl_yr_wk_caption_1033_txt, fscl_wk_caption_1033_txt
    from ax_fscl_calendar
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_FSCL_CLNDR') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'FSCL_CLNDR') and _load != 3%}
        union
        select
        fscl_clndr_sk, fscl_clndr_ck, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        busn_apprvd_ind, clndr_dt, dt_ak, serial_mnth_nbr, fscl_yr_id, fscl_yr_nm, fscl_yr_mnth_id, fscl_yr_mnth_nm, fscl_mnth_nbr, fscl_mnth_nm, cn_to_us_cnvrsn_rt, fscl_mnth_strt_dt, fscl_mnth_end_dt, prev_fscl_mnth_strt_dt, prev_fscl_mnth_end_dt, fscl_yr_strt_dt, fscl_yr_end_dt, fscl_mnth_days_nbr, fscl_period_id, fscl_wk_day_nbr, wk_day_ind, clndr_yr_nbr, clndr_yr_day_nbr, clndr_qtr_nbr, clndr_yr_qtr_nbr, clndr_qtr_day_nbr, clndr_qtr_mnth_nbr, clndr_mnth_nbr, clndr_yr_mnth_nbr, clndr_mnth_day_nbr, clndr_wk_yr_nbr, clndr_yr_wk_day_nbr, clndr_wk_nbr, clndr_yr_wk_nbr, fscl_yr_nbr, fscl_yr_day_nbr, fscl_qtr_nbr, fscl_yr_qtr_nbr, fscl_qtr_day_nbr, fscl_qtr_mnth_nbr, fscl_yr_mnth_nbr, fscl_mnth_nbr_1, fscl_mnth_day_nbr, fscl_wk_yr_nbr, fscl_yr_wk_day_nbr, fscl_yr_wk_nbr, fscl_wk_nbr, dt_caption_1033_txt, wk_day_caption_1033_txt, clndr_yr_caption_1033_txt, clndr_qtr_caption_1033_txt, clndr_yr_qtr_caption_1033_txt, clndr_mnth_caption_1033_txt, clndr_yr_mnth_caption_1033_txt, clndr_wk_yr_caption_1033_txt, clndr_wk_caption_1033_txt, clndr_yr_wk_caption_1033_txt, fscl_yr_caption_1033_txt, fscl_qtr_caption_1033_txt, fscl_yr_qtr_caption_1033_txt, fscl_mnth_caption_1033_txt, fscl_yr_mnth_caption_1033_txt, fscl_wk_yr_caption_1033_txt, fscl_yr_wk_caption_1033_txt, fscl_wk_caption_1033_txt
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.FSCL_CLNDR
        {% if _load == 1 %}
            where FSCL_CLNDR_ck not in (select distinct FSCL_CLNDR_ck from final)
        {% else %}
            where FSCL_CLNDR_ck not in (select distinct FSCL_CLNDR_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.FSCL_CLNDR)
        {% endif %} 
    {% endif %}
)
select * from final


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and FSCL_CLNDR_ck not in (select distinct FSCL_CLNDR_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.FSCL_CLNDR)) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and FSCL_CLNDR_ck in (select distinct FSCL_CLNDR_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.FSCL_CLNDR) and delete_dtm is not null)
    {% endif %}
{% endif %}