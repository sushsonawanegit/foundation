{% set _load = load_type('AX_OPCO_PROJECT_TRANS_POSTING') %}

with ax_opco_project_trans_posting as (
    select
    current_timestamp as crt_dtm,
    ptp._fivetran_synced as stg_load_dtm,
    case
        when ptp._fivetran_deleted = true then ptp._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    ptp.dataareaid as opco_id,
    ptp.recid as src_key_txt,
    opr.opco_project_sk,
    opco.opco_sk,
    oi.opco_item_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    opc.opco_project_catgry_sk,
    opto.opco_project_trans_origin_sk,
    opto1.opco_project_trans_origin_sk as opco_gl_trans_origin_sk,
    ogtp.opco_gl_trans_posting_type_sk,
    ocoa.opco_chart_of_accts_sk as opco_gl_acct_sk,
    ptp.transid as project_trans_posting_id,
    ptp.ledgertransdate as gl_trans_dt,
    ptp.voucher as voucher_nbr,
    ptp.projtransdate as project_trans_dt,
    case 
        when ptp.projtranstype = 0 then null
        when ptp.projtranstype = 1 then 'Fee'
        when ptp.projtranstype = 2 then 'Hour'
        when ptp.projtranstype = 3 then 'Expense'
        when ptp.projtranstype = 4 then 'Item'
        when ptp.projtranstype = 5 then 'On Account'
        when ptp.projtranstype = 6 then 'WIP'
    end as project_trans_type_txt,
    case
        when ptp.projtype = 0 then 'Time and Material'
        when ptp.projtype = 1 then 'Fixed Price'
        when ptp.projtype = 2 then 'Investment'
        when ptp.projtype = 3 then 'Cost Project'
        when ptp.projtype = 4 then 'Internal'
        when ptp.projtype = 5 then 'Time Project'
        when ptp.projtype = 6 then 'Summary'
    end as project_type_txt,
    case
        when ptp.costsales = 0 then null
        when ptp.costsales = 1 then 'Cost'
        when ptp.costsales = 2 then 'Sales'
    end as project_cost_or_sls_txt,
    ptp.projadjustrefid as project_adjstmt_ref_id,
    ptp.paymentdate as pymnt_dt,
    case 
        when ptp.paymentstatus = 0 then 'No Payment'
        when ptp.paymentstatus = 1 then 'Expected Payment'
        when ptp.paymentstatus = 2 then 'Paid'
    end as pymnt_status_txt,
    ptp.inventtransid as invtry_trans_id,
    case
        when ptp.projtranstype = 2 then ptp.emplitemid
        else null
    end as empl_id,
    ptp.qty as trans_qty,
    ptp.amountmst as opco_currency_trans_amt
    from {{ source('AX_DEV', 'PROJTRANSPOSTING') }} ptp
    left join {{ ref('ax_opco_project_curr')}} opr
        on ptp.dataareaid = opr.opco_id
        and ptp.projid = opr.project_id
        and opr.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_curr')}} opco 
        on ptp.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_item_curr')}} oi 
        on ptp.emplitemid = oi.src_item_cd
        and ptp.dataareaid = oi.opco_id
        and oi.src_sys_nm = 'AX'
        and ptp.projtranstype = 4
        and ptp.emplitemid is not null
    left join {{ref('ax_opco_cost_center_curr')}} occ 
        on upper(ptp.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ref('ax_opco_dept_curr')}} od 
        on upper(ptp.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ref('ax_opco_type_curr')}} ot 
        on upper(ptp.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ref('ax_opco_purpose_curr')}} op 
        on upper(ptp.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join {{ref('ax_opco_lob_curr')}} ol
        on upper(ptp.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_project_catgry_curr')}} opc
        on ptp.dataareaid = opc.opco_id
        and ptp.categoryid = opc.catgry_cd
        and opc.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_project_trans_origin_curr')}} opto
        on ptp.transactionorigin = opto.project_trans_origin_cd
        and opto.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_project_trans_origin_curr')}} opto1
        on ptp.ledgerorigin = opto1.project_trans_origin_cd
        and opto1.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_gl_trans_posting_type_curr')}} ogtp
        on ptp.postingtype = ogtp.src_gl_trans_posting_type_cd
        and ogtp.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_chart_of_accts_curr')}} ocoa
        on ptp.dataareaid = ocoa.opco_id
        and ptp.account = ocoa.gl_acct_nbr
        and ocoa.src_sys_nm = 'AX'
    where ptp.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select 
        {{ dbt_utils.surrogate_key(['src_sys_nm','opco_id','src_key_txt']) }} as opco_project_trans_posting_sk,
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_key_txt', 'opco_project_sk', 'opco_sk', 'opco_item_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_project_catgry_sk', 'opco_project_trans_origin_sk', 'opco_gl_trans_origin_sk', 'opco_gl_trans_posting_type_sk', 'opco_gl_acct_sk', 'project_trans_posting_id', 'gl_trans_dt', 'voucher_nbr', 'project_trans_dt', 'project_trans_type_txt', 'project_type_txt', 'project_cost_or_sls_txt', 'project_adjstmt_ref_id', 'pymnt_dt', 'pymnt_status_txt', 'invtry_trans_id', 'empl_id', 'trans_qty', 'opco_currency_trans_amt']) }} as opco_project_trans_posting_ck,
        concat_ws('|', src_sys_nm, opco_id, src_key_txt) as opco_project_trans_posting_ak,
        * 		
    from ax_opco_project_trans_posting optp															
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_PROJECT_TRANS_POSTING') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PROJECT_TRANS_POSTING') and _load != 3%}
    union
    select
    OPCO_PROJECT_TRANS_POSTING_sk, OPCO_PROJECT_TRANS_POSTING_ck,OPCO_PROJECT_TRANS_POSTING_ak, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, opco_id, src_key_txt, opco_project_sk, opco_sk, opco_item_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_project_catgry_sk, opco_project_trans_origin_sk, opco_gl_trans_origin_sk, opco_gl_trans_posting_type_sk, opco_gl_acct_sk, project_trans_posting_id, gl_trans_dt, voucher_nbr, project_trans_dt, project_trans_type_txt, project_type_txt, project_cost_or_sls_txt, project_adjstmt_ref_id, pymnt_dt, pymnt_status_txt, invtry_trans_id, empl_id, trans_qty, opco_currency_trans_amt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_TRANS_POSTING
    {% if _load == 1 %}
        where OPCO_PROJECT_TRANS_POSTING_ck not in (select distinct OPCO_PROJECT_TRANS_POSTING_ck from final)
    {% else %}
        where OPCO_PROJECT_TRANS_POSTING_ck not in (select distinct OPCO_PROJECT_TRANS_POSTING_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_TRANS_POSTING where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PROJECT_TRANS_POSTING_ck not in (select distinct OPCO_PROJECT_TRANS_POSTING_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_TRANS_POSTING where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PROJECT_TRANS_POSTING_ck in (select distinct OPCO_PROJECT_TRANS_POSTING_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_TRANS_POSTING where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}