with opco_cost_center as(
    select *,
    rank() over(partition by opco_cost_center_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_cost_center_lineage') }}
),
final as(
    select 
    OPCO_COST_CENTER_SK, OPCO_COST_CENTER_CK, CRT_DTM, STG_LOAD_DTM, DELETE_DTM, SRC_SYS_NM, SRC_COST_CENTER_CD, SRC_COST_CENTER_DESC, COST_CENTER_WO_SPCL_CHR_CD, ACTV_IND, SRC_COST_CENTER_TYPE_TXT
    from opco_cost_center
    where rnk = 1 and delete_dtm is null
)

select * from final

