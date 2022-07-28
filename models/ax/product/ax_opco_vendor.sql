{% set _load = load_type('AX_OPCO_VENDOR') %}

with ax_opco_vendor as(
    select 
    current_timestamp as crt_dtm,
    vt._fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then vt._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(vt.accountnum) as vendor_id,
    vt.name as vendor_nm,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    opm.opco_pymnt_mode_sk,
    opt.opco_pymnt_terms_sk,
    odm.opco_dlvry_mode_sk,
    oc.opco_currency_sk,
    '' as vendor_grp_cd,
    vt.namealias as vendor_alt_nm,
    vt.dba as vendor_dba_nm,
    vt.foreignentityindicator as foreign_entity_ind,
    case    
        when vt.blocked = 0 then 'No'
        when vt.blocked = 1 then 'Invoice'
        when vt.blocked = 2 then 'All'
    end as vendor_block_txt
    from {{source('AX_DEV', 'VENDTABLE')}} vt
    left join {{ ref('opco_cost_center')}} occ 
        on upper(vt.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ ref('opco_dept')}} od 
        on upper(vt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join {{ ref('opco_type')}} ot 
        on upper(vt.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join {{ ref('opco_purpose')}} op 
        on upper(vt.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join {{ ref('opco_lob')}} ol
        on upper(vt.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join {{ref('opco_pymnt_mode')}} opm 
        on upper(vt.paymmode) = opm.src_pymnt_mode_cd
        and opm.src_sys_nm = 'AX'
    left join {{ref('opco_pymnt_terms')}} opt 
        on upper(vt.paymtermid) = opt.src_pymnt_terms_cd
        and opt.src_sys_nm = 'AX'
    left join {{ ref('opco_currency')}} oc 
        on upper(vt.currency) = oc.src_currency_cd
        and oc.src_sys_nm = 'AX'
    left join {{ ref('opco_dlvry_mode')}} odm 
        on upper(vt.dlvmode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    where vt.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'vendor_id']) }} as opco_vendor_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'vendor_id', 'vendor_nm', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_pymnt_mode_sk', 'opco_pymnt_terms_sk', 'opco_dlvry_mode_sk', 'opco_currency_sk', 'vendor_grp_cd', 'vendor_alt_nm', 'vendor_dba_nm', 'foreign_entity_ind', 'vendor_block_txt',]) }} as OPCO_VENDOR_ck,
            * 
    from ax_opco_vendor
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_VENDOR') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_VENDOR') and _load != 3%}
    union
    select
    OPCO_VENDOR_SK, OPCO_VENDOR_CK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, vendor_id, vendor_nm, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_pymnt_mode_sk, opco_pymnt_terms_sk, opco_dlvry_mode_sk, opco_currency_sk, vendor_grp_cd, namealias as vendor_alt_nm, vendor_dba_nm, foreign_entity_ind, vendor_block_txt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR
    {% if _load == 1 %}
        where OPCO_VENDOR_ck not in (select distinct OPCO_VENDOR_ck from final)
    {% else %}
        where OPCO_VENDOR_ck not in (select distinct OPCO_VENDOR_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_VENDOR_ck not in (select distinct OPCO_VENDOR_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_VENDOR_ck in (select distinct OPCO_VENDOR_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_VENDOR where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}