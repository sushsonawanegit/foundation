{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_gl_trans_posting_type as (
  {% set values = [ 'Exchange rate profit', 'Exchange rate loss', 'Intercompany accounting', 'Sales tax', 'Sales tax rounding', 'Allocation', 'Investment tax', 'Liquidity', 'Penny difference in secondary currency', 'Error account', 'Penny difference in default currency', 'Year-end result', 'Close', 'Ledger journal', 'Cash discount', 'Balance account for consolidation differences', 'Payment fee', 'Sales tax reporting', 'Transfer of closing and opening transactions', 'Bank', 'Conversion profit', 'Conversion loss', 'Withholding tax', 'Profit & loss account for consolidation differences', 'Production - indirect WIP offset', 'Production - indirect absorption', 'Production - indirect absorption offset', 'Customer balance', 'Customer revenue', 'Customer interest', 'Customer cash discount', 'Customer collection letter fee', 'Customer interest fee', 'Customer invoice discount', 'Customer payment', 'Reimbursement', 'Customer settlement', 'Vendor balance', 'Vendor Incoming', 'Vendor offset account', 'Vendor interest', 'Vendor cash discount', 'Vendor disbursement', 'Vendor invoice discount', 'Vendor settlement', 'Intercompany settlement', 'Fixed asset issue', 'Sales order revenue', 'Sales order consumption', 'Sales order discount', 'Order cash', 'Order, freight', 'Order fee', 'Sales order postage', 'Order invoice rounding', 'Order packing slip', 'Order offset account packing slip', 'Sales order issue', 'Sales, commission', 'Sales, commission offset', 'Sales - packing slip revenue', 'Sales - packing slip revenue offset', 'Purchase, consumption', 'Purchase, discount', 'Purchase cash', 'Purchase freight', 'Purchase fee', 'Purchase postage', 'Purchase offset account', 'Purchase invoice rounding-off', 'Purchase misc. charges, freight', 'Purchase misc. charges duty', 'Purchase misc. charges insurance', 'Purchase, packing slip', 'Purchase, packing slip offset', 'Purchase, receipt', 'Purchase, fixed receipt price profit', 'Purchase, fixed receipt price loss', 'Purchase, fixed receipt price offset', 'Inventory receipt', 'Inventory issue', 'Inventory profit', 'Inventory loss', 'Inventory, fixed receipt price profit', 'Inventory, fixed receipt price loss', 'Production, report as finished', 'Production offset-account, report as finished', 'Production issue', 'Production offset account issue', 'Production receipt', 'Production offset account receipt', 'Production offset account picking list', 'Production, picking list', 'Production - WIP', 'Production WIP issue', 'Production - Work center issue', 'Production scrap', 'Production offset account, work center issue', 'Production offset account, scrap', 'Project - Cost', 'Project - Payroll allocation', 'Project - WIP cost', 'Project - Cost - Item', 'Project - WIP cost - Item', 'Project - Invoiced revenue', 'Project - Invoiced on-account', 'Project - Accrued revenue - Sales value', 'Project - WIP - Sales value', 'Project - Accrued revenue - Production', 'Project - WIP - Production', 'Project - Accrued revenue - Profit', 'Project - WIP - Profit', 'Never ledger', 'Project - Accrued loss', 'Project - WIP - Accrued loss', 'Project - Accrued revenue - On account', 'Project - WIP invoiced - On account', 'No ledger', 'Wage debit account', 'Payroll Credit account', 'Fixed assets, debit', 'Fixed assets, credit', 'Ledger journal - no offset account', 'Purchase, charge', 'Purchase, stock variation', 'Purchase, packing slip purchase offset', 'Purchase, packing slip tax', 'Purchase, packing slip purchase', 'Sales, packing slip tax', 'Accrued revenue - Subscription', 'WIP - Subscription', 'Cost change variance', 'System rounding', 'Purchase price variance', 'Production price variance', 'Inventory inter-unit payable', 'Inventory inter-unit receivable', 'Production - indirect WIP issue', 'Production lot size variance', 'Production quantity variance', 'Production substitution variance', 'Rounding variance', 'Fixed asset receipt', 'Inventory cost revaluation' ] %}
  {% set num = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 91, 92, 93, 94, 95, 96, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 141, 142, 161, 162, 163, 201, 202, 203, 204, 205, 206, 207, 208, 210, 211, 213, 216, 219, 220, 221, 222, 223, 224, 225, 226, 235] %}

  {% for i in range(145) %}
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          {{num[i]}} as src_gl_trans_posting_type_cd,
          {% if i == 0 %}
            null as src_gl_trans_posting_type_desc
          {% else %}
            '{{values[i-1]}}' as src_gl_trans_posting_type_desc
          {% endif %}
          {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_gl_trans_posting_type_cd']) }} as opco_gl_trans_posting_type_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_gl_trans_posting_type_cd', 'src_gl_trans_posting_type_desc']) }} as opco_gl_trans_posting_type_ck, 
            *
    from opco_gl_trans_posting_type
)

select * from final