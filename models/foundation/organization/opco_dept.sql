with opco_dept as(
    select *,
    rank() over(partition by opco_dept_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_dept_lineage') }}
),
final as(
    select 
    OPCO_DEPT_SK, OPCO_DEPT_CK, CRT_DTM, STG_LOAD_DTM, DELETE_DTM, SRC_SYS_NM, SRC_DEPT_CD, SRC_DEPT_DESC, DEPT_WO_SPCL_CHR_CD, ACTV_IND
    from opco_dept
    where rnk = 1 and delete_dtm is null
)

select * from final

