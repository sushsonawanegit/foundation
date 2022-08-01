

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_gl_trans_posting_type  as
      (

with opco_gl_trans_posting_type as (
  
  

  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          0 as src_gl_trans_posting_type_cd,
          
            null as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          1 as src_gl_trans_posting_type_cd,
          
            'Exchange rate profit' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          2 as src_gl_trans_posting_type_cd,
          
            'Exchange rate loss' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          3 as src_gl_trans_posting_type_cd,
          
            'Intercompany accounting' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          4 as src_gl_trans_posting_type_cd,
          
            'Sales tax' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          5 as src_gl_trans_posting_type_cd,
          
            'Sales tax rounding' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          6 as src_gl_trans_posting_type_cd,
          
            'Allocation' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          7 as src_gl_trans_posting_type_cd,
          
            'Investment tax' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          8 as src_gl_trans_posting_type_cd,
          
            'Liquidity' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          9 as src_gl_trans_posting_type_cd,
          
            'Penny difference in secondary currency' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          10 as src_gl_trans_posting_type_cd,
          
            'Error account' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          11 as src_gl_trans_posting_type_cd,
          
            'Penny difference in default currency' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          12 as src_gl_trans_posting_type_cd,
          
            'Year-end result' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          13 as src_gl_trans_posting_type_cd,
          
            'Close' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          14 as src_gl_trans_posting_type_cd,
          
            'Ledger journal' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          15 as src_gl_trans_posting_type_cd,
          
            'Cash discount' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          16 as src_gl_trans_posting_type_cd,
          
            'Balance account for consolidation differences' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          17 as src_gl_trans_posting_type_cd,
          
            'Payment fee' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          18 as src_gl_trans_posting_type_cd,
          
            'Sales tax reporting' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          19 as src_gl_trans_posting_type_cd,
          
            'Transfer of closing and opening transactions' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          20 as src_gl_trans_posting_type_cd,
          
            'Bank' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          21 as src_gl_trans_posting_type_cd,
          
            'Conversion profit' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          22 as src_gl_trans_posting_type_cd,
          
            'Conversion loss' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          23 as src_gl_trans_posting_type_cd,
          
            'Withholding tax' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          24 as src_gl_trans_posting_type_cd,
          
            'Profit & loss account for consolidation differences' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          25 as src_gl_trans_posting_type_cd,
          
            'Production - indirect WIP offset' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          26 as src_gl_trans_posting_type_cd,
          
            'Production - indirect absorption' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          27 as src_gl_trans_posting_type_cd,
          
            'Production - indirect absorption offset' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          31 as src_gl_trans_posting_type_cd,
          
            'Customer balance' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          32 as src_gl_trans_posting_type_cd,
          
            'Customer revenue' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          33 as src_gl_trans_posting_type_cd,
          
            'Customer interest' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          34 as src_gl_trans_posting_type_cd,
          
            'Customer cash discount' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          35 as src_gl_trans_posting_type_cd,
          
            'Customer collection letter fee' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          36 as src_gl_trans_posting_type_cd,
          
            'Customer interest fee' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          37 as src_gl_trans_posting_type_cd,
          
            'Customer invoice discount' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          38 as src_gl_trans_posting_type_cd,
          
            'Customer payment' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          39 as src_gl_trans_posting_type_cd,
          
            'Reimbursement' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          40 as src_gl_trans_posting_type_cd,
          
            'Customer settlement' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          41 as src_gl_trans_posting_type_cd,
          
            'Vendor balance' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          42 as src_gl_trans_posting_type_cd,
          
            'Vendor Incoming' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          43 as src_gl_trans_posting_type_cd,
          
            'Vendor offset account' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          44 as src_gl_trans_posting_type_cd,
          
            'Vendor interest' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          45 as src_gl_trans_posting_type_cd,
          
            'Vendor cash discount' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          46 as src_gl_trans_posting_type_cd,
          
            'Vendor disbursement' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          47 as src_gl_trans_posting_type_cd,
          
            'Vendor invoice discount' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          48 as src_gl_trans_posting_type_cd,
          
            'Vendor settlement' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          49 as src_gl_trans_posting_type_cd,
          
            'Intercompany settlement' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          50 as src_gl_trans_posting_type_cd,
          
            'Fixed asset issue' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          51 as src_gl_trans_posting_type_cd,
          
            'Sales order revenue' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          52 as src_gl_trans_posting_type_cd,
          
            'Sales order consumption' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          53 as src_gl_trans_posting_type_cd,
          
            'Sales order discount' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          54 as src_gl_trans_posting_type_cd,
          
            'Order cash' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          55 as src_gl_trans_posting_type_cd,
          
            'Order, freight' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          56 as src_gl_trans_posting_type_cd,
          
            'Order fee' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          57 as src_gl_trans_posting_type_cd,
          
            'Sales order postage' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          58 as src_gl_trans_posting_type_cd,
          
            'Order invoice rounding' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          59 as src_gl_trans_posting_type_cd,
          
            'Order packing slip' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          60 as src_gl_trans_posting_type_cd,
          
            'Order offset account packing slip' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          61 as src_gl_trans_posting_type_cd,
          
            'Sales order issue' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          62 as src_gl_trans_posting_type_cd,
          
            'Sales, commission' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          63 as src_gl_trans_posting_type_cd,
          
            'Sales, commission offset' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          64 as src_gl_trans_posting_type_cd,
          
            'Sales - packing slip revenue' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          65 as src_gl_trans_posting_type_cd,
          
            'Sales - packing slip revenue offset' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          71 as src_gl_trans_posting_type_cd,
          
            'Purchase, consumption' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          72 as src_gl_trans_posting_type_cd,
          
            'Purchase, discount' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          73 as src_gl_trans_posting_type_cd,
          
            'Purchase cash' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          74 as src_gl_trans_posting_type_cd,
          
            'Purchase freight' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          75 as src_gl_trans_posting_type_cd,
          
            'Purchase fee' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          76 as src_gl_trans_posting_type_cd,
          
            'Purchase postage' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          77 as src_gl_trans_posting_type_cd,
          
            'Purchase offset account' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          78 as src_gl_trans_posting_type_cd,
          
            'Purchase invoice rounding-off' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          79 as src_gl_trans_posting_type_cd,
          
            'Purchase misc. charges, freight' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          80 as src_gl_trans_posting_type_cd,
          
            'Purchase misc. charges duty' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          81 as src_gl_trans_posting_type_cd,
          
            'Purchase misc. charges insurance' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          82 as src_gl_trans_posting_type_cd,
          
            'Purchase, packing slip' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          83 as src_gl_trans_posting_type_cd,
          
            'Purchase, packing slip offset' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          84 as src_gl_trans_posting_type_cd,
          
            'Purchase, receipt' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          85 as src_gl_trans_posting_type_cd,
          
            'Purchase, fixed receipt price profit' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          86 as src_gl_trans_posting_type_cd,
          
            'Purchase, fixed receipt price loss' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          87 as src_gl_trans_posting_type_cd,
          
            'Purchase, fixed receipt price offset' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          91 as src_gl_trans_posting_type_cd,
          
            'Inventory receipt' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          92 as src_gl_trans_posting_type_cd,
          
            'Inventory issue' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          93 as src_gl_trans_posting_type_cd,
          
            'Inventory profit' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          94 as src_gl_trans_posting_type_cd,
          
            'Inventory loss' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          95 as src_gl_trans_posting_type_cd,
          
            'Inventory, fixed receipt price profit' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          96 as src_gl_trans_posting_type_cd,
          
            'Inventory, fixed receipt price loss' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          105 as src_gl_trans_posting_type_cd,
          
            'Production, report as finished' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          106 as src_gl_trans_posting_type_cd,
          
            'Production offset-account, report as finished' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          107 as src_gl_trans_posting_type_cd,
          
            'Production issue' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          108 as src_gl_trans_posting_type_cd,
          
            'Production offset account issue' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          109 as src_gl_trans_posting_type_cd,
          
            'Production receipt' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          110 as src_gl_trans_posting_type_cd,
          
            'Production offset account receipt' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          111 as src_gl_trans_posting_type_cd,
          
            'Production offset account picking list' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          112 as src_gl_trans_posting_type_cd,
          
            'Production, picking list' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          113 as src_gl_trans_posting_type_cd,
          
            'Production - WIP' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          114 as src_gl_trans_posting_type_cd,
          
            'Production WIP issue' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          115 as src_gl_trans_posting_type_cd,
          
            'Production - Work center issue' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          116 as src_gl_trans_posting_type_cd,
          
            'Production scrap' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          117 as src_gl_trans_posting_type_cd,
          
            'Production offset account, work center issue' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          118 as src_gl_trans_posting_type_cd,
          
            'Production offset account, scrap' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          121 as src_gl_trans_posting_type_cd,
          
            'Project - Cost' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          122 as src_gl_trans_posting_type_cd,
          
            'Project - Payroll allocation' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          123 as src_gl_trans_posting_type_cd,
          
            'Project - WIP cost' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          124 as src_gl_trans_posting_type_cd,
          
            'Project - Cost - Item' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          125 as src_gl_trans_posting_type_cd,
          
            'Project - WIP cost - Item' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          126 as src_gl_trans_posting_type_cd,
          
            'Project - Invoiced revenue' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          127 as src_gl_trans_posting_type_cd,
          
            'Project - Invoiced on-account' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          128 as src_gl_trans_posting_type_cd,
          
            'Project - Accrued revenue - Sales value' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          129 as src_gl_trans_posting_type_cd,
          
            'Project - WIP - Sales value' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          130 as src_gl_trans_posting_type_cd,
          
            'Project - Accrued revenue - Production' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          131 as src_gl_trans_posting_type_cd,
          
            'Project - WIP - Production' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          132 as src_gl_trans_posting_type_cd,
          
            'Project - Accrued revenue - Profit' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          133 as src_gl_trans_posting_type_cd,
          
            'Project - WIP - Profit' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          134 as src_gl_trans_posting_type_cd,
          
            'Never ledger' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          135 as src_gl_trans_posting_type_cd,
          
            'Project - Accrued loss' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          136 as src_gl_trans_posting_type_cd,
          
            'Project - WIP - Accrued loss' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          137 as src_gl_trans_posting_type_cd,
          
            'Project - Accrued revenue - On account' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          138 as src_gl_trans_posting_type_cd,
          
            'Project - WIP invoiced - On account' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          139 as src_gl_trans_posting_type_cd,
          
            'No ledger' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          141 as src_gl_trans_posting_type_cd,
          
            'Wage debit account' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          142 as src_gl_trans_posting_type_cd,
          
            'Payroll Credit account' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          161 as src_gl_trans_posting_type_cd,
          
            'Fixed assets, debit' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          162 as src_gl_trans_posting_type_cd,
          
            'Fixed assets, credit' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          163 as src_gl_trans_posting_type_cd,
          
            'Ledger journal - no offset account' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          201 as src_gl_trans_posting_type_cd,
          
            'Purchase, charge' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          202 as src_gl_trans_posting_type_cd,
          
            'Purchase, stock variation' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          203 as src_gl_trans_posting_type_cd,
          
            'Purchase, packing slip purchase offset' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          204 as src_gl_trans_posting_type_cd,
          
            'Purchase, packing slip tax' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          205 as src_gl_trans_posting_type_cd,
          
            'Purchase, packing slip purchase' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          206 as src_gl_trans_posting_type_cd,
          
            'Sales, packing slip tax' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          207 as src_gl_trans_posting_type_cd,
          
            'Accrued revenue - Subscription' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          208 as src_gl_trans_posting_type_cd,
          
            'WIP - Subscription' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          210 as src_gl_trans_posting_type_cd,
          
            'Cost change variance' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          211 as src_gl_trans_posting_type_cd,
          
            'System rounding' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          213 as src_gl_trans_posting_type_cd,
          
            'Purchase price variance' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          216 as src_gl_trans_posting_type_cd,
          
            'Production price variance' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          219 as src_gl_trans_posting_type_cd,
          
            'Inventory inter-unit payable' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          220 as src_gl_trans_posting_type_cd,
          
            'Inventory inter-unit receivable' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          221 as src_gl_trans_posting_type_cd,
          
            'Production - indirect WIP issue' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          222 as src_gl_trans_posting_type_cd,
          
            'Production lot size variance' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          223 as src_gl_trans_posting_type_cd,
          
            'Production quantity variance' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          224 as src_gl_trans_posting_type_cd,
          
            'Production substitution variance' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          225 as src_gl_trans_posting_type_cd,
          
            'Rounding variance' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          226 as src_gl_trans_posting_type_cd,
          
            'Fixed asset receipt' as src_gl_trans_posting_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          235 as src_gl_trans_posting_type_cd,
          
            'Inventory cost revaluation' as src_gl_trans_posting_type_desc
          
          
  
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_gl_trans_posting_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_posting_type_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_gl_trans_posting_type_cd as 
    varchar
), '') || '-' || coalesce(cast(src_gl_trans_posting_type_desc as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_posting_type_ck, 
            *
    from opco_gl_trans_posting_type
)

select * from final
      );
    