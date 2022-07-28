with fscl_clndr as(
    select *,
    rank() over(partition by fscl_clndr_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_fscl_clndr_lineage') }}
),
final as(
    select 
    fscl_clndr_SK, fscl_clndr_CK, CRT_DTM, STG_LOAD_DTM, DELETE_DTM, busn_apprvd_ind, clndr_dt, dt_ak, serial_mnth_nbr, fscl_yr_id, fscl_yr_nm, fscl_yr_mnth_id, fscl_yr_mnth_nm, fscl_mnth_nbr, fscl_mnth_nm, cn_to_us_cnvrsn_rt, fscl_mnth_strt_dt, fscl_mnth_end_dt, prev_fscl_mnth_strt_dt, prev_fscl_mnth_end_dt, fscl_yr_strt_dt, fscl_yr_end_dt, fscl_mnth_days_nbr, fscl_period_id, fscl_wk_day_nbr, wk_day_ind, clndr_yr_nbr, clndr_yr_day_nbr, clndr_qtr_nbr, clndr_yr_qtr_nbr, clndr_qtr_day_nbr, clndr_qtr_mnth_nbr, clndr_mnth_nbr, clndr_yr_mnth_nbr, clndr_mnth_day_nbr, clndr_wk_yr_nbr, clndr_yr_wk_day_nbr, clndr_wk_nbr, clndr_yr_wk_nbr, fscl_yr_nbr, fscl_yr_day_nbr, fscl_qtr_nbr, fscl_yr_qtr_nbr, fscl_qtr_day_nbr, fscl_qtr_mnth_nbr, fscl_yr_mnth_nbr, fscl_mnth_nbr_1, fscl_mnth_day_nbr, fscl_wk_yr_nbr, fscl_yr_wk_day_nbr, fscl_yr_wk_nbr, fscl_wk_nbr, dt_caption_1033_txt, wk_day_caption_1033_txt, clndr_yr_caption_1033_txt, clndr_qtr_caption_1033_txt, clndr_yr_qtr_caption_1033_txt, clndr_mnth_caption_1033_txt, clndr_yr_mnth_caption_1033_txt, clndr_wk_yr_caption_1033_txt, clndr_wk_caption_1033_txt, clndr_yr_wk_caption_1033_txt, fscl_yr_caption_1033_txt, fscl_qtr_caption_1033_txt, fscl_yr_qtr_caption_1033_txt, fscl_mnth_caption_1033_txt, fscl_yr_mnth_caption_1033_txt, fscl_wk_yr_caption_1033_txt, fscl_yr_wk_caption_1033_txt, fscl_wk_caption_1033_txt
    from fscl_clndr
    where rnk = 1 and delete_dtm is null
)

select * from final

