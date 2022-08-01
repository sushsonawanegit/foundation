{% set _load = load_type('AX_OPCO_ITEM_OPCO_ASSCTN_XREF') %}


with ax_opco_item_opco_assctn_xref as(
    select 
    oi.opco_item_sk,
    oa.opco_assctn_sk,
    current_timestamp as crt_dtm,
    iil._fivetran_synced as stg_load_dtm,
    case
        when iil._fivetran_deleted = true then iil._fivetran_synced
        else null
    end as delete_dtm,
    '' as opco_item_count_grp_sk,
    '' as opco_item_scrap_type_sk,
    'AX' as src_sys_nm,
    iil.binlocation_opi as bin_locn_txt,
    iil.buildinglocation_opi as bldg_locn_txt,
    iil.cyds_opi as cubic_yrds_amt,
    iil.cydsfixed_opi as cubic_yrds_fixed_amt,
    iil.cydsvariable_opi as cubic_yrds_variable_amt,
    iil.fixedweight_opi as fixed_wt_amt,
    iil.glprodqty_opi as gl_prodn_qty,
    iil.glsalesqty_opi as gl_sls_qty,
    iil.laborhours_opi as labor_hrs_nbr,
    iil.maxqtyonhand_opi as max_on_hand_qty,
    iil.netweight_opi as net_wt_amt,
    iil.prodabcd_opi as prodn_abcd_cd,
    iil.prodflushingprinciple_opi as prodn_flushing_principle_cd,
    iil.variableweight_opi as variable_wt_amt
    from {{source('AX_DEV', 'INVENTITEMLOCATION')}} iil
    left join {{ref('ax_opco_item_curr')}} oi 
        on iil.itemid = oi.src_item_cd
        and iil.dataareaid = oi.opco_id
        and oi.src_sys_nm = 'AX'
    left join {{ref('ax_opco_assctn_curr')}} oa 
        on iil.dataareaid = oa.opco_id
        and iil.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    where iil.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select distinct 
        opco_item_sk, opco_assctn_sk, 
        {{ dbt_utils.surrogate_key(['opco_item_count_grp_sk', 'opco_item_scrap_type_sk', 'src_sys_nm', 'bin_locn_txt', 'bldg_locn_txt', 'cubic_yrds_amt', 'cubic_yrds_fixed_amt', 'cubic_yrds_variable_amt', 'fixed_wt_amt', 'gl_prodn_qty', 'gl_sls_qty', 'labor_hrs_nbr', 'max_on_hand_qty', 'net_wt_amt', 'prodn_abcd_cd', 'prodn_flushing_principle_cd', 'variable_wt_amt']) }} as opco_item_opco_assctn_xref_ck,
        crt_dtm, stg_load_dtm, delete_dtm, opco_item_count_grp_sk, opco_item_scrap_type_sk, src_sys_nm, bin_locn_txt, bldg_locn_txt, cubic_yrds_amt, cubic_yrds_fixed_amt, cubic_yrds_variable_amt, fixed_wt_amt, gl_prodn_qty, gl_sls_qty, labor_hrs_nbr, max_on_hand_qty, net_wt_amt, prodn_abcd_cd, prodn_flushing_principle_cd, variable_wt_amt
    from ax_opco_item_opco_assctn_xref 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_ITEM_OPCO_ASSCTN_XREF') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_ITEM_OPCO_ASSCTN_XREF') and _load != 3%}
    union
    select
    opco_item_sk, opco_assctn_sk, opco_item_opco_assctn_xref_ck, crt_dtm, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    opco_item_count_grp_sk, opco_item_scrap_type_sk, src_sys_nm, bin_locn_txt, bldg_locn_txt, cubic_yrds_amt, cubic_yrds_fixed_amt, cubic_yrds_variable_amt, fixed_wt_amt, gl_prodn_qty, gl_sls_qty, labor_hrs_nbr, max_on_hand_qty, net_wt_amt, prodn_abcd_cd, prodn_flushing_principle_cd, variable_wt_amt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_OPCO_ASSCTN_XREF
    {% if _load == 1 %}
        where OPCO_ITEM_OPCO_ASSCTN_XREF_ck not in (select distinct OPCO_ITEM_OPCO_ASSCTN_XREF_ck from final)
    {% else %}
        where OPCO_ITEM_OPCO_ASSCTN_XREF_ck not in (select distinct OPCO_ITEM_OPCO_ASSCTN_XREF_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_OPCO_ASSCTN_XREF where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_OPCO_ASSCTN_XREF_ck not in (select distinct OPCO_ITEM_OPCO_ASSCTN_XREF_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_OPCO_ASSCTN_XREF where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_OPCO_ASSCTN_XREF_ck in (select distinct OPCO_ITEM_OPCO_ASSCTN_XREF_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_OPCO_ASSCTN_XREF where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  