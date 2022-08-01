

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_trans_origin  as
      (

with opco_project_trans_origin as (
  
  

  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          0 as project_trans_origin_cd,
          'None' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          1 as project_trans_origin_cd,
          'Hour Journal' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          4 as project_trans_origin_cd,
          'Expense Journal' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          5 as project_trans_origin_cd,
          'General Journal' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          6 as project_trans_origin_cd,
          'Invoice Journal' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          7 as project_trans_origin_cd,
          'Invoice Approval Journal' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          8 as project_trans_origin_cd,
          'Expense Management' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          9 as project_trans_origin_cd,
          'Elimination Investment' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          10 as project_trans_origin_cd,
          'Estimate Accrued Loss' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          12 as project_trans_origin_cd,
          'Item Journal' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          13 as project_trans_origin_cd,
          'Purchase Order' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          14 as project_trans_origin_cd,
          'Item Requirement' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          15 as project_trans_origin_cd,
          'Sales Order' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          16 as project_trans_origin_cd,
          'Production-FInished' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          17 as project_trans_origin_cd,
          'Production-Consumed' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          18 as project_trans_origin_cd,
          'Fee Journal' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          19 as project_trans_origin_cd,
          'Estimate Fee' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          20 as project_trans_origin_cd,
          'Subscription' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          21 as project_trans_origin_cd,
          'Prepayment' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          22 as project_trans_origin_cd,
          'Deduction' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          23 as project_trans_origin_cd,
          'Milestone' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          50 as project_trans_origin_cd,
          'Invoice' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          51 as project_trans_origin_cd,
          'Inventory Closing' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          52 as project_trans_origin_cd,
          'Adjustment' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          53 as project_trans_origin_cd,
          'Post Cost' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          54 as project_trans_origin_cd,
          'Accrue Revenue' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          55 as project_trans_origin_cd,
          'Post Estimate' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          56 as project_trans_origin_cd,
          'Reverse Estimate' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          57 as project_trans_origin_cd,
          'Eliminate Estimate' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          58 as project_trans_origin_cd,
          'Reverse Elimination' as project_trans_origin_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          59 as project_trans_origin_cd,
          'Accrue Subscription Revenue' as project_trans_origin_desc
          
  
),
final as(
    select 
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(project_trans_origin_cd as 
    varchar
), '') as 
    varchar
)) as opco_project_trans_origin_sk,
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(project_trans_origin_cd as 
    varchar
), '') || '-' || coalesce(cast(project_trans_origin_desc as 
    varchar
), '') as 
    varchar
)) as opco_project_trans_origin_ck,
        *
    from opco_project_trans_origin
)

select * from final
      );
    