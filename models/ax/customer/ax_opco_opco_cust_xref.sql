{% set _load = load_type('AX_OPCO_OPCO_CUST_XREF') %}

with ax_opco_opco_cust_xref as(
    select 
    opco.opco_sk,
    oc.opco_cust_sk,
    current_timestamp as crt_dtm,
    ct._fivetran_synced as stg_load_dtm,
    case
        when ct._fivetran_deleted = true then ct._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    1 as actv_ind,
    opcur.opco_currency_sk,
    oppyt.opco_pymnt_terms_sk,
    oppym.opco_pymnt_mode_sk,
    opcc.opco_cust_code_sk,
    opct.opco_cust_type_sk,
    opcg.opco_commsn_grp_sk,
    opcug.opco_cust_grp_sk,
    opcdt.opco_cash_dscnt_terms_sk,
    upper(ct.linedisc) as line_disc_txt,
    ct.creditmax as max_cr_amt,
    ct.mandatorycreditlimit as mandatory_cr_lmt_ind,
    ct.blocked as block_lvl_cd,
    ct.custclassificationid as cust_clssfctn_cd,
    upper(ct.paymsched) as pymnt_schd_desc,
    ct.salesgroup as sls_grp_nm,
    ct.regsalesmanager_opi as rgnl_sls_mgr_nm,
    ct.invoiceaccount as invoice_acct_nbr,
    ct.segment_opi as segment_cd,
    ct.segmentation_opi as segmentation_txt
    from {{source('AX_DEV', 'CUSTTABLE')}} ct
    left join {{ref('ax_opco_curr')}} opco
        on ct.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ref('ax_opco_cust_curr')}} oc 
        on ct.accountnum = oc.cust_id
        and ct.dataareaid = oc.opco_id
        and oc.src_sys_nm = 'AX' 
    left join {{ref('ax_opco_currency_curr')}} opcur
        on upper(ct.currency) = opcur.src_currency_cd
        and opcur.src_sys_nm = 'AX'
    left join {{ref('ax_opco_pymnt_terms_curr')}} oppyt
        on upper(ct.paymtermid) = oppyt.src_pymnt_terms_cd
        and oppyt.src_sys_nm = 'AX'
    left join {{ref('ax_opco_pymnt_mode_curr')}} oppym
        on  upper(ct.paymmode) = oppym.src_pymnt_mode_cd 
        and oppym.src_sys_nm = 'AX'
    left join {{ref('ax_opco_cust_code_curr')}} opcc 
        on upper(ct.precastcode_opi) = opcc.src_cust_code_cd 
        and opcc.src_sys_nm = 'AX'
    left join {{ref('ax_opco_cust_type_curr')}} opct
        on upper(ct.precastcategory_opi) = opct.src_cust_type_cd
        and opct.src_sys_nm = 'AX' 
    left join {{ref('ax_opco_commsn_grp_curr')}} opcg 
        on upper(ct.commissiongroup) = opcg.src_commsn_grp_cd
        and opcg.src_sys_nm = 'AX'
    left join {{ref('ax_opco_cust_grp_curr')}} opcug
        on upper(ct.custgroup) = opcug.src_cust_grp_cd 
        and opcug.src_sys_nm = 'AX'
    left join {{ref('ax_opco_cash_dscnt_terms_curr')}} opcdt 
        on upper(cashdisc) = opcdt.src_cash_dscnt_terms_cd
        and opcdt.src_sys_nm = 'AX' 
    where dataareaid not in {{ var('excluded_ax_companies')}}
),
final as(
    select
        {{ dbt_utils.surrogate_key([ 'opco_sk',  'opco_cust_sk',  'actv_ind',  'opco_currency_sk',  'opco_pymnt_terms_sk',  'opco_pymnt_mode_sk',  'opco_cust_code_sk',  'opco_cust_type_sk',  'opco_commsn_grp_sk',  'opco_cust_grp_sk',  'opco_cash_dscnt_terms_sk',  'line_disc_txt',  'max_cr_amt',  'mandatory_cr_lmt_ind',  'block_lvl_cd',  'cust_clssfctn_cd',  'pymnt_schd_desc',  'sls_grp_nm',  'rgnl_sls_mgr_nm',  'invoice_acct_nbr',  'segment_cd',  'segmentation_txt'])}} as opco_opco_cust_xref_ck,
        *
    from ax_opco_opco_cust_xref
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_OPCO_CUST_XREF') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_OPCO_CUST_XREF') and _load != 3%}
    union
    select
    opco_sk, opco_cust_sk, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, actv_ind, opco_currency_sk, opco_pymnt_terms_sk, opco_pymnt_mode_sk, opco_cust_code_sk, opco_cust_type_sk, opco_commsn_grp_sk, opco_cust_grp_sk, opco_cash_dscnt_terms_sk, line_disc_txt, max_cr_amt, mandatory_cr_lmt_ind, block_lvl_cd, cust_clssfctn_cd, pymnt_schd_desc, sls_grp_nm, rgnl_sls_mgr_nm, invoice_acct_nbr, segment_cd, segmentation_txt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_OPCO_CUST_XREF
    {% if _load == 1 %}
        where OPCO_OPCO_CUST_XREF_ck not in (select distinct OPCO_OPCO_CUST_XREF_ck from final)
    {% else %}
        where OPCO_OPCO_CUST_XREF_ck not in (select distinct OPCO_OPCO_CUST_XREF_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_OPCO_CUST_XREF where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}

{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_OPCO_CUST_XREF_ck not in (select distinct OPCO_OPCO_CUST_XREF_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_OPCO_CUST_XREF where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_OPCO_CUST_XREF_ck in (select distinct OPCO_OPCO_CUST_XREF_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_OPCO_CUST_XREF where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}