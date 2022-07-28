with opco_vendor as(
    select *,
    rank() over(partition by opco_vendor_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_vendor_lineage') }}
),
final as(
    select 
    OPCO_VENDOR_SK, OPCO_VENDOR_CK, CRT_DTM,
    STG_LOAD_DTM,DELETE_DTM,
    src_sys_nm, vendor_id, vendor_nm, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_pymnt_mode_sk, opco_pymnt_terms_sk, opco_dlvry_mode_sk, opco_currency_sk, vendor_grp_cd, vendor_alt_nm, vendor_dba_nm, foreign_entity_ind, vendor_block_txt
 
    from opco_vendor
    where rnk = 1 and delete_dtm is null
)

select * from final
