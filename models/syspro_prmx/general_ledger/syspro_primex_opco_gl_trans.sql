-- depends_on: {{ ref('syspro_primex_opco_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_chart_of_accts_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_gl_trans_type_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_cost_center_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_type_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_dept_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_purpose_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_lob_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_uom_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_sub_ledger_type_curr') }}
-- depends_on: {{ ref('syspro_primex_opco_currency_curr') }}

{% set _load = load_type('SYSPRO_PRIMEX_OPCO_GL_TRANS') %}

with syspro_primex_opco_gl_trans as(
    {% set tables = table_check('COMPANY', '_DBO_GENJOURNALDETAIL') %}
    {% for tbl in tables %}
        select
        '{{ tbl}}' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        {{ dbt_utils.surrogate_key(["'SYSPRO-PRMX'", 'src_comp', 'trim(gjd.glyear)', 'trim(gjd.glperiod)', 'upper(trim(gjd.journal))', 'trim(gjd.entrynumber)']) }} as opco_gl_trans_sk, 
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
        ifnull(oc.opco_currency_sk, (select opco_currency_sk from {{ ref('syspro_primex_opco_curr')}} where src_sys_nm = 'SYSPRO-PRMX' and opco_id = '{{ tbl}}')) as trans_currency_sk,
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
        from {{ var('primex_db')}}.{{ var('primex_schema')}}.COMPANY{{ tbl}}_DBO_GENJOURNALDETAIL gjd
        left join {{ var('primex_db')}}.{{ var('primex_schema')}}.COMPANY{{ tbl}}_DBO_CUSGENMASTER_ cgm
            on trim(gjd.glcode) = trim(cgm.glcode)
        left join {{ var('primex_db')}}.{{ var('primex_schema')}}.COMPANY{{ tbl}}_DBO_GENMASTER cm
            on trim(gjd.company) = trim(cm.company)
            and trim(gjd.glcode) = trim(cm.glcode)
        left join {{ ref('syspro_primex_opco_curr')}} opco 
            on opco.opco_id = '{{tbl}}'
            and opco.src_sys_nm = 'SYSPRO-PRMX'
        left join {{ ref('syspro_primex_opco_chart_of_accts_curr')}} ocoa 
            on ocoa.opco_id = '{{tbl}}'
            and ocoa.gl_acct_nbr = upper(trim(gjd.glcode))
            and ocoa.src_sys_nm = 'SYSPRO-PRMX'
        left join {{ ref('syspro_primex_opco_gl_trans_type_curr')}} ogtt 
            on ogtt.src_gl_trans_type_cd = upper(trim(gjd.source))
            and ogtt.src_sys_nm = 'SYSPRO-PRMX'
        left join {{ ref('syspro_primex_opco_cost_center_curr')}} occ 
            on occ.src_cost_center_cd = upper(trim(cgm.cc))
            and occ.src_sys_nm = 'SYSPRO-PRMX'
        left join {{ ref('syspro_primex_opco_dept_curr')}} od 
            on od.src_dept_cd = upper(trim(cgm.dept))
            and od.src_sys_nm = 'SYSPRO-PRMX'
        left join {{ ref('syspro_primex_opco_type_curr')}} ot 
            on ot.src_type_cd = upper(trim(cgm.type))
            and ot.src_sys_nm = 'SYSPRO-PRMX'
        left join {{ ref('syspro_primex_opco_purpose_curr')}} op
            on op.src_purpose_cd = upper(trim(cgm.purpose))
            and op.src_sys_nm = 'SYSPRO-PRMX'
        left join {{ ref('syspro_primex_opco_lob_curr')}} ol
            on ol.src_lob_cd = upper(trim(cgm.fsgroup4buname))
            and ol.src_sys_nm = 'SYSPRO-PRMX'
        left join {{ ref('syspro_primex_opco_uom_curr')}} ou 
            on ou.src_uom_cd = upper(trim(cm.glunitofmeasure))
            and ou.src_sys_nm = 'SYSPRO-PRMX'
        left join {{ ref('syspro_primex_opco_sub_ledger_type_curr')}} oslt 
            on oslt.src_sub_ledger_type_cd = upper(trim(gjd.type))
            and oslt.src_sys_nm = 'SYSPRO-PRMX'
        left join {{ ref('syspro_primex_opco_currency_curr')}} oc 
            on oc.src_currency_cd = upper(trim(gjd.postcurrency))
            and oc.src_sys_nm = 'SYSPRO-PRMX'
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
),
de_duplication as(
    select *
           , rank() over(partition by opco_gl_trans_ak order by stg_load_dtm, src_comp) as rnk
    from syspro_primex_opco_gl_trans
),
final as(
    select  distinct 
            OPCO_GL_TRANS_SK,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_key_txt', 'trans_dt', 'opco_sk', 'opco_chart_of_accts_sk', 'opco_gl_trans_type_sk', 'opco_gl_trans_posting_type_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'trans_currency_sk', 'opco_proj_sk', 'original_gl_opco_sk', 'original_gl_voucher_nbr', 'voucher_nbr', 'journal_batch_nbr', 'gl_desc', 'document_nbr', 'document_dt', 'crrctn_trans_ind', 'credit_ind', 'gl_qty', 'trans_currency_gl_amt', 'opco_currency_gl_amt', 'fscl_yr_nbr', 'fscl_mnth_nbr', 'posted_ind', 'opco_uom_sk', 'opco_brand_sk', 'opco_sub_ledger_type_sk', 'sub_ledger_cd'])}} as OPCO_GL_TRANS_CK,
            OPCO_GL_TRANS_AK, 
            CRT_DTM, STG_LOAD_DTM, DELETE_DTM, SRC_SYS_NM, SRC_KEY_TXT, TRANS_DT, OPCO_SK, OPCO_CHART_OF_ACCTS_SK, OPCO_GL_TRANS_TYPE_SK, OPCO_GL_TRANS_POSTING_TYPE_SK, OPCO_COST_CENTER_SK, OPCO_DEPT_SK, OPCO_TYPE_SK, OPCO_PURPOSE_SK, OPCO_LOB_SK, TRANS_CURRENCY_SK, OPCO_PROJ_SK, ORIGINAL_GL_OPCO_SK, ORIGINAL_GL_VOUCHER_NBR, VOUCHER_NBR, JOURNAL_BATCH_NBR, GL_DESC, DOCUMENT_NBR, DOCUMENT_DT, CRRCTN_TRANS_IND, CREDIT_IND, GL_QTY, TRANS_CURRENCY_GL_AMT, OPCO_CURRENCY_GL_AMT, FSCL_YR_NBR, FSCL_MNTH_NBR, POSTED_IND, OPCO_UOM_SK, OPCO_BRAND_SK, OPCO_SUB_LEDGER_TYPE_SK, SUB_LEDGER_CD
    from de_duplication
    where rnk = 1 
),
delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'SYSPRO_PRIMEX_OPCO_GL_TRANS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_GL_TRANS') and _load != 3%}
        union
        select
        OPCO_GL_TRANS_sk, OPCO_GL_TRANS_ck,  OPCO_GL_TRANS_AK, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        SRC_SYS_NM, SRC_KEY_TXT, TRANS_DT, OPCO_SK, OPCO_CHART_OF_ACCTS_SK, OPCO_GL_TRANS_TYPE_SK, OPCO_GL_TRANS_POSTING_TYPE_SK, OPCO_COST_CENTER_SK, OPCO_DEPT_SK, OPCO_TYPE_SK, OPCO_PURPOSE_SK, OPCO_LOB_SK, TRANS_CURRENCY_SK, OPCO_PROJ_SK, ORIGINAL_GL_OPCO_SK, ORIGINAL_GL_VOUCHER_NBR, VOUCHER_NBR, JOURNAL_BATCH_NBR, GL_DESC, DOCUMENT_NBR, DOCUMENT_DT, CRRCTN_TRANS_IND, CREDIT_IND, GL_QTY, TRANS_CURRENCY_GL_AMT, OPCO_CURRENCY_GL_AMT, FSCL_YR_NBR, FSCL_MNTH_NBR, POSTED_IND, OPCO_UOM_SK, OPCO_BRAND_SK, OPCO_SUB_LEDGER_TYPE_SK, SUB_LEDGER_CD
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS
        {% if _load == 1 %}
            where OPCO_GL_TRANS_ck not in (select distinct OPCO_GL_TRANS_ck from final)
        {% else %}
            where OPCO_GL_TRANS_ck not in (select distinct OPCO_GL_TRANS_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS where src_sys_nm = 'SYSPRO-PRMX')
        {% endif %} 
        and src_sys_nm = 'SYSPRO-PRMX'
    {% endif %}
)

select * from delete_captured


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_GL_TRANS_ck not in (select distinct OPCO_GL_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_GL_TRANS_ck in (select distinct OPCO_GL_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_GL_TRANS where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    {% endif %}
{% endif %}