with v_opco_project_trans_origin_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PROJECT_TRANS_ORIGIN_SK" as character varying(32)) as "OPCO_PROJECT_TRANS_ORIGIN_SK" ,
                    cast("OPCO_PROJECT_TRANS_ORIGIN_CK" as character varying(32)) as "OPCO_PROJECT_TRANS_ORIGIN_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("PROJECT_TRANS_ORIGIN_CD" as NUMBER(2,0)) as "PROJECT_TRANS_ORIGIN_CD" ,
                    cast("PROJECT_TRANS_ORIGIN_DESC" as character varying(27)) as "PROJECT_TRANS_ORIGIN_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_trans_origin

            
        )

        
)

select * from v_opco_project_trans_origin_lineage