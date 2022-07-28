{% set _load = load_type('AX_OPCO_ITEM_PSI_DTL') %}

with ax_opco_item_psi_dtl as (
    select
    current_timestamp as crt_dtm, 
    it._fivetran_synced as stg_load_dtm,
    case
        when it._fivetran_deleted = true then it._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    it.itemid as src_item_cd,
    it.dataareaid as opco_id,
    case 
        when it.moduletype =  0 then 'Inventory'
        when it.moduletype =  1 then 'Purchase Order'
        when it.moduletype =  2 then 'Sales Order'
    end as psi_type_txt,
    oi.opco_item_sk,
    ou.opco_uom_sk,
    it.pricedate as psi_price_dt,
    it.price as psi_price_amt,
    it.priceunit as psi_price_unit_qty,
    it.del_quantity as psi_dlvry_qty,
    it.linedisc as psi_line_dscnt_amt
    from {{source('AX_DEV', 'INVENTTABLEMODULE')}} it 
    left join {{ref('opco_item')}} oi 
        on it.dataareaid = oi.opco_id 
        and it.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join {{ref('opco_uom')}} ou 
        on upper(it.unitid) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    where it.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_item_cd', 'psi_type_txt']) }} as opco_item_psi_dtl_sk,
            {{ dbt_utils.surrogate_key([ 'src_sys_nm',  'src_item_cd',  'opco_id',  'psi_type_txt',  'opco_item_sk',  'opco_uom_sk',  'psi_price_dt',  'psi_price_amt',  'psi_price_unit_qty',  'psi_dlvry_qty',  'psi_line_dscnt_amt']) }} as opco_item_psi_dtl_ck,
            concat_ws('|', src_sys_nm, opco_id, src_item_cd, psi_type_txt) as opco_item_psi_dtl_ak, 
            * 
    from ax_opco_item_psi_dtl 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_ITEM_PSI_DTL') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_ITEM_PSI_DTL') and _load != 3%}
    union
    select
    OPCO_ITEM_PSI_DTL_SK, OPCO_ITEM_PSI_DTL_CK, opco_item_psi_dtl_ak, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
    src_sys_nm, opco_id, src_item_cd, psi_type_txt, psi_price_dt, psi_price_amt, psi_price_unit_qty, psi_dlvry_qty, psi_line_dscnt_amt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_PSI_DTL
    {% if _load == 1 %}
        where OPCO_ITEM_PSI_DTL_ck not in (select distinct OPCO_ITEM_PSI_DTL_ck from final)
    {% else %}
        where OPCO_ITEM_PSI_DTL_ck not in (select distinct OPCO_ITEM_PSI_DTL_ck from final) 
             and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_PSI_DTL where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_PSI_DTL_ck not in (select distinct OPCO_ITEM_PSI_DTL_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_PSI_DTL where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_ITEM_PSI_DTL_ck in (select distinct OPCO_ITEM_PSI_DTL_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_ITEM_PSI_DTL where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}  