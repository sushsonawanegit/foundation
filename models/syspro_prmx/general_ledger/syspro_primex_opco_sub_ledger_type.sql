{% set _load = load_type('SYSPRO_PRIMEX_OPCO_SUB_LEDGER_TYPE') %}

with syspro_opco_sub_ledger_type as(
    {% set tables = table_check('COMPANY', '_DBO_GENJOURNALDETAIL') %}
    {% for tbl in tables %}
        select
        '{{ tbl}}' as src_comp,
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'SYSPRO-PRMX' as src_sys_nm,
        upper(trim(type)) as src_sub_ledger_type_cd,
        case 
            when trim(type) = 'BIL' then 'Billing'
            when trim(type) = 'CASH' then 'Cash'
            when trim(type) = 'CSBK' then 'Cashbook'
            when trim(type) = 'DBS' then 'Disbursement'
            when trim(type) = 'EXP' then 'Expense'
            when trim(type) = 'GRN' then 'Goods Receive Note'
            when trim(type) = 'INV' then 'Inventory'
            when trim(type) = 'LAB' then 'Labour'
            when trim(type) = 'SALE' then 'Sales'
            else trim(type)  
        end as src_sub_ledger_type_desc,
        1::number(1,0) as actv_ind
        from {{ var('primex_db')}}.{{ var('primex_schema')}}.COMPANY{{ tbl}}_DBO_GENJOURNALDETAIL
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
),
de_duplication as(
    select *
           , rank() over(partition by src_sub_ledger_type_cd order by src_comp) as rnk
    from syspro_opco_sub_ledger_type
),
final as(
    select distinct
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_sub_ledger_type_cd']) }} as opco_sub_ledger_type_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_sub_ledger_type_cd', 'src_sub_ledger_type_desc', 'actv_ind']) }} as opco_sub_ledger_type_ck,
            crt_dtm,stg_load_dtm, delete_dtm, src_sys_nm, src_sub_ledger_type_cd, src_sub_ledger_type_desc, actv_ind            
    from de_duplication
    where rnk = 1 
),
delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'SYSPRO_PRIMEX_OPCO_SUB_LEDGER_TYPE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_SUB_LEDGER_TYPE') and _load != 3%}
        union
        select
        OPCO_SUB_LEDGER_TYPE_SK, OPCO_SUB_LEDGER_TYPE_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        SRC_SYS_NM, src_sub_ledger_type_cd, src_sub_ledger_type_desc, actv_ind
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SUB_LEDGER_TYPE
        {% if _load == 1 %}
            where OPCO_SUB_LEDGER_TYPE_ck not in (select distinct OPCO_SUB_LEDGER_TYPE_ck from final)
        {% else %}
            where OPCO_SUB_LEDGER_TYPE_ck not in (select distinct OPCO_SUB_LEDGER_TYPE_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SUB_LEDGER_TYPE where src_sys_nm = 'SYSPRO-PRMX')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_SUB_LEDGER_TYPE_ck not in (select distinct OPCO_SUB_LEDGER_TYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SUB_LEDGER_TYPE where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_SUB_LEDGER_TYPE_ck in (select distinct OPCO_SUB_LEDGER_TYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_SUB_LEDGER_TYPE where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    {% endif %}
{% endif %}