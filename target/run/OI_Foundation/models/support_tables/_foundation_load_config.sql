

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp._foundation_load_config  as
      (with _foundation_load_config as(
    
    

    
        select 
        'AX_OPCO_CASH_DSCNT_TERMS' as model_nm,
        replace('AX_OPCO_CASH_DSCNT_TERMS', concat(split_part('AX_OPCO_CASH_DSCNT_TERMS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_CASH_DSCNT_TERMS', concat(split_part('AX_OPCO_CASH_DSCNT_TERMS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_CASH_DSCNT_TERMS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_CASH_DSCNT_TERMS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_CASH_DSCNT_TERMS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_CASH_DSCNT_TERMS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_CASH_DSCNT_TERMS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_CASH_DSCNT_TERMS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_CASH_DSCNT_TERMS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_CASH_DSCNT_TERMS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_CASH_DSCNT_TERMS
        
         union all 
    
        select 
        'AX_OPCO_COMMSN_GRP' as model_nm,
        replace('AX_OPCO_COMMSN_GRP', concat(split_part('AX_OPCO_COMMSN_GRP', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_COMMSN_GRP', concat(split_part('AX_OPCO_COMMSN_GRP', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_COMMSN_GRP', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_COMMSN_GRP', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_COMMSN_GRP', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_COMMSN_GRP', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_COMMSN_GRP', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_COMMSN_GRP', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_COMMSN_GRP', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_COMMSN_GRP', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_COMMSN_GRP
        
         union all 
    
        select 
        'AX_OPCO_CURRENCY' as model_nm,
        replace('AX_OPCO_CURRENCY', concat(split_part('AX_OPCO_CURRENCY', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_CURRENCY', concat(split_part('AX_OPCO_CURRENCY', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_CURRENCY', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_CURRENCY', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_CURRENCY', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_CURRENCY', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_CURRENCY', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_CURRENCY', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_CURRENCY', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_CURRENCY', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_CURRENCY
        
         union all 
    
        select 
        'AX_OPCO_CUST_CODE' as model_nm,
        replace('AX_OPCO_CUST_CODE', concat(split_part('AX_OPCO_CUST_CODE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_CUST_CODE', concat(split_part('AX_OPCO_CUST_CODE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_CUST_CODE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_CUST_CODE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_CUST_CODE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_CUST_CODE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_CUST_CODE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_CUST_CODE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_CUST_CODE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_CUST_CODE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_CUST_CODE
        
         union all 
    
        select 
        'AX_OPCO_CUST_GRP' as model_nm,
        replace('AX_OPCO_CUST_GRP', concat(split_part('AX_OPCO_CUST_GRP', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_CUST_GRP', concat(split_part('AX_OPCO_CUST_GRP', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_CUST_GRP', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_CUST_GRP', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_CUST_GRP', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_CUST_GRP', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_CUST_GRP', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_CUST_GRP', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_CUST_GRP', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_CUST_GRP', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_CUST_GRP
        
         union all 
    
        select 
        'AX_OPCO_CUST_INVOICE_LINE' as model_nm,
        replace('AX_OPCO_CUST_INVOICE_LINE', concat(split_part('AX_OPCO_CUST_INVOICE_LINE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_CUST_INVOICE_LINE', concat(split_part('AX_OPCO_CUST_INVOICE_LINE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_CUST_INVOICE_LINE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_CUST_INVOICE_LINE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_CUST_INVOICE_LINE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_CUST_INVOICE_LINE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_CUST_INVOICE_LINE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_CUST_INVOICE_LINE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_CUST_INVOICE_LINE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_CUST_INVOICE_LINE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_CUST_INVOICE_LINE
        
         union all 
    
        select 
        'AX_OPCO_CUST_INVOICE' as model_nm,
        replace('AX_OPCO_CUST_INVOICE', concat(split_part('AX_OPCO_CUST_INVOICE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_CUST_INVOICE', concat(split_part('AX_OPCO_CUST_INVOICE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_CUST_INVOICE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_CUST_INVOICE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_CUST_INVOICE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_CUST_INVOICE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_CUST_INVOICE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_CUST_INVOICE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_CUST_INVOICE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_CUST_INVOICE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_CUST_INVOICE
        
         union all 
    
        select 
        'AX_OPCO_CUST_OPEN_TRANS' as model_nm,
        replace('AX_OPCO_CUST_OPEN_TRANS', concat(split_part('AX_OPCO_CUST_OPEN_TRANS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_CUST_OPEN_TRANS', concat(split_part('AX_OPCO_CUST_OPEN_TRANS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_CUST_OPEN_TRANS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_CUST_OPEN_TRANS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_CUST_OPEN_TRANS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_CUST_OPEN_TRANS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_CUST_OPEN_TRANS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_CUST_OPEN_TRANS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_CUST_OPEN_TRANS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_CUST_OPEN_TRANS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_CUST_OPEN_TRANS
        
         union all 
    
        select 
        'AX_OPCO_CUST_PACKING_SLIP_LINE' as model_nm,
        replace('AX_OPCO_CUST_PACKING_SLIP_LINE', concat(split_part('AX_OPCO_CUST_PACKING_SLIP_LINE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_CUST_PACKING_SLIP_LINE', concat(split_part('AX_OPCO_CUST_PACKING_SLIP_LINE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_CUST_PACKING_SLIP_LINE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_CUST_PACKING_SLIP_LINE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_CUST_PACKING_SLIP_LINE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_CUST_PACKING_SLIP_LINE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_CUST_PACKING_SLIP_LINE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_CUST_PACKING_SLIP_LINE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_CUST_PACKING_SLIP_LINE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_CUST_PACKING_SLIP_LINE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_CUST_PACKING_SLIP_LINE
        
         union all 
    
        select 
        'AX_OPCO_CUST_PACKING_SLIP' as model_nm,
        replace('AX_OPCO_CUST_PACKING_SLIP', concat(split_part('AX_OPCO_CUST_PACKING_SLIP', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_CUST_PACKING_SLIP', concat(split_part('AX_OPCO_CUST_PACKING_SLIP', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_CUST_PACKING_SLIP', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_CUST_PACKING_SLIP', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_CUST_PACKING_SLIP', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_CUST_PACKING_SLIP', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_CUST_PACKING_SLIP', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_CUST_PACKING_SLIP', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_CUST_PACKING_SLIP', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_CUST_PACKING_SLIP', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_CUST_PACKING_SLIP
        
         union all 
    
        select 
        'AX_OPCO_CUST_SETTLED_TRANS' as model_nm,
        replace('AX_OPCO_CUST_SETTLED_TRANS', concat(split_part('AX_OPCO_CUST_SETTLED_TRANS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_CUST_SETTLED_TRANS', concat(split_part('AX_OPCO_CUST_SETTLED_TRANS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_CUST_SETTLED_TRANS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_CUST_SETTLED_TRANS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_CUST_SETTLED_TRANS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_CUST_SETTLED_TRANS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_CUST_SETTLED_TRANS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_CUST_SETTLED_TRANS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_CUST_SETTLED_TRANS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_CUST_SETTLED_TRANS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_CUST_SETTLED_TRANS
        
         union all 
    
        select 
        'AX_OPCO_CUST_TRANS' as model_nm,
        replace('AX_OPCO_CUST_TRANS', concat(split_part('AX_OPCO_CUST_TRANS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_CUST_TRANS', concat(split_part('AX_OPCO_CUST_TRANS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_CUST_TRANS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_CUST_TRANS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_CUST_TRANS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_CUST_TRANS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_CUST_TRANS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_CUST_TRANS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_CUST_TRANS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_CUST_TRANS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_CUST_TRANS
        
         union all 
    
        select 
        'AX_OPCO_CUST_TYPE' as model_nm,
        replace('AX_OPCO_CUST_TYPE', concat(split_part('AX_OPCO_CUST_TYPE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_CUST_TYPE', concat(split_part('AX_OPCO_CUST_TYPE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_CUST_TYPE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_CUST_TYPE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_CUST_TYPE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_CUST_TYPE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_CUST_TYPE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_CUST_TYPE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_CUST_TYPE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_CUST_TYPE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_CUST_TYPE
        
         union all 
    
        select 
        'AX_OPCO_CUST' as model_nm,
        replace('AX_OPCO_CUST', concat(split_part('AX_OPCO_CUST', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_CUST', concat(split_part('AX_OPCO_CUST', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_CUST', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_CUST', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_CUST', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_CUST', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_CUST', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_CUST', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_CUST', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_CUST', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_CUST
        
         union all 
    
        select 
        'AX_OPCO_OPCO_CUST_XREF' as model_nm,
        replace('AX_OPCO_OPCO_CUST_XREF', concat(split_part('AX_OPCO_OPCO_CUST_XREF', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_OPCO_CUST_XREF', concat(split_part('AX_OPCO_OPCO_CUST_XREF', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_OPCO_CUST_XREF', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_OPCO_CUST_XREF', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_OPCO_CUST_XREF', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_OPCO_CUST_XREF', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_OPCO_CUST_XREF', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_OPCO_CUST_XREF', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_OPCO_CUST_XREF', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_OPCO_CUST_XREF', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_OPCO_CUST_XREF
        
         union all 
    
        select 
        'AX_OPCO_PICKING_LIST' as model_nm,
        replace('AX_OPCO_PICKING_LIST', concat(split_part('AX_OPCO_PICKING_LIST', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PICKING_LIST', concat(split_part('AX_OPCO_PICKING_LIST', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PICKING_LIST', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PICKING_LIST', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PICKING_LIST', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PICKING_LIST', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PICKING_LIST', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PICKING_LIST', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PICKING_LIST', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PICKING_LIST', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PICKING_LIST
        
         union all 
    
        select 
        'AX_OPCO_PYMNT_MODE' as model_nm,
        replace('AX_OPCO_PYMNT_MODE', concat(split_part('AX_OPCO_PYMNT_MODE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PYMNT_MODE', concat(split_part('AX_OPCO_PYMNT_MODE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PYMNT_MODE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PYMNT_MODE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PYMNT_MODE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PYMNT_MODE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PYMNT_MODE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PYMNT_MODE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PYMNT_MODE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PYMNT_MODE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PYMNT_MODE
        
         union all 
    
        select 
        'AX_OPCO_PYMNT_TERMS' as model_nm,
        replace('AX_OPCO_PYMNT_TERMS', concat(split_part('AX_OPCO_PYMNT_TERMS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PYMNT_TERMS', concat(split_part('AX_OPCO_PYMNT_TERMS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PYMNT_TERMS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PYMNT_TERMS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PYMNT_TERMS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PYMNT_TERMS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PYMNT_TERMS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PYMNT_TERMS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PYMNT_TERMS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PYMNT_TERMS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PYMNT_TERMS
        
         union all 
    
        select 
        'AX_CHART_OF_ACCTS' as model_nm,
        replace('AX_CHART_OF_ACCTS', concat(split_part('AX_CHART_OF_ACCTS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_CHART_OF_ACCTS', concat(split_part('AX_CHART_OF_ACCTS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_CHART_OF_ACCTS', '_', 1) = 'AX' then 3
                when split_part('AX_CHART_OF_ACCTS', '_', 1) = 'NS' then 3
                when split_part('AX_CHART_OF_ACCTS', '_', 1) = 'JDE' then 1
                when split_part('AX_CHART_OF_ACCTS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_CHART_OF_ACCTS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_CHART_OF_ACCTS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_CHART_OF_ACCTS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_CHART_OF_ACCTS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_CHART_OF_ACCTS
        
         union all 
    
        select 
        'AX_OPCO_CHART_OF_ACCTS' as model_nm,
        replace('AX_OPCO_CHART_OF_ACCTS', concat(split_part('AX_OPCO_CHART_OF_ACCTS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_CHART_OF_ACCTS', concat(split_part('AX_OPCO_CHART_OF_ACCTS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_CHART_OF_ACCTS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_CHART_OF_ACCTS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_CHART_OF_ACCTS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_CHART_OF_ACCTS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_CHART_OF_ACCTS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_CHART_OF_ACCTS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_CHART_OF_ACCTS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_CHART_OF_ACCTS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_CHART_OF_ACCTS
        
         union all 
    
        select 
        'AX_OPCO_FSCL_YR_GL_OPENING_BAL' as model_nm,
        replace('AX_OPCO_FSCL_YR_GL_OPENING_BAL', concat(split_part('AX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_FSCL_YR_GL_OPENING_BAL', concat(split_part('AX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_FSCL_YR_GL_OPENING_BAL
        
         union all 
    
        select 
        'AX_OPCO_GL_TRANS' as model_nm,
        replace('AX_OPCO_GL_TRANS', concat(split_part('AX_OPCO_GL_TRANS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_GL_TRANS', concat(split_part('AX_OPCO_GL_TRANS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_GL_TRANS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_GL_TRANS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_GL_TRANS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_GL_TRANS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_GL_TRANS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_GL_TRANS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_GL_TRANS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_GL_TRANS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_GL_TRANS
        
         union all 
    
        select 
        'AX_OPCO_CUST_OPCO_LOCN_XREF' as model_nm,
        replace('AX_OPCO_CUST_OPCO_LOCN_XREF', concat(split_part('AX_OPCO_CUST_OPCO_LOCN_XREF', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_CUST_OPCO_LOCN_XREF', concat(split_part('AX_OPCO_CUST_OPCO_LOCN_XREF', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_CUST_OPCO_LOCN_XREF', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_CUST_OPCO_LOCN_XREF', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_CUST_OPCO_LOCN_XREF', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_CUST_OPCO_LOCN_XREF', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_CUST_OPCO_LOCN_XREF', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_CUST_OPCO_LOCN_XREF', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_CUST_OPCO_LOCN_XREF', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_CUST_OPCO_LOCN_XREF', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_CUST_OPCO_LOCN_XREF
        
         union all 
    
        select 
        'AX_OPCO_LOCN' as model_nm,
        replace('AX_OPCO_LOCN', concat(split_part('AX_OPCO_LOCN', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_LOCN', concat(split_part('AX_OPCO_LOCN', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_LOCN', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_LOCN', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_LOCN', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_LOCN', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_LOCN', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_LOCN', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_LOCN', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_LOCN', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_LOCN
        
         union all 
    
        select 
        'AX_OPCO_OPCO_LOCN_XREF' as model_nm,
        replace('AX_OPCO_OPCO_LOCN_XREF', concat(split_part('AX_OPCO_OPCO_LOCN_XREF', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_OPCO_LOCN_XREF', concat(split_part('AX_OPCO_OPCO_LOCN_XREF', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_OPCO_LOCN_XREF', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_OPCO_LOCN_XREF', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_OPCO_LOCN_XREF', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_OPCO_LOCN_XREF', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_OPCO_LOCN_XREF', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_OPCO_LOCN_XREF', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_OPCO_LOCN_XREF', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_OPCO_LOCN_XREF', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_OPCO_LOCN_XREF
        
         union all 
    
        select 
        'AX_OPCO_VENDOR_OPCO_LOCN_XREF' as model_nm,
        replace('AX_OPCO_VENDOR_OPCO_LOCN_XREF', concat(split_part('AX_OPCO_VENDOR_OPCO_LOCN_XREF', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_VENDOR_OPCO_LOCN_XREF', concat(split_part('AX_OPCO_VENDOR_OPCO_LOCN_XREF', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_VENDOR_OPCO_LOCN_XREF', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_VENDOR_OPCO_LOCN_XREF', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_VENDOR_OPCO_LOCN_XREF', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_VENDOR_OPCO_LOCN_XREF', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_VENDOR_OPCO_LOCN_XREF', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_VENDOR_OPCO_LOCN_XREF', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_VENDOR_OPCO_LOCN_XREF', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_VENDOR_OPCO_LOCN_XREF', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_VENDOR_OPCO_LOCN_XREF
        
         union all 
    
        select 
        'AX_FSCL_CLNDR' as model_nm,
        replace('AX_FSCL_CLNDR', concat(split_part('AX_FSCL_CLNDR', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_FSCL_CLNDR', concat(split_part('AX_FSCL_CLNDR', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_FSCL_CLNDR', '_', 1) = 'AX' then 3
                when split_part('AX_FSCL_CLNDR', '_', 1) = 'NS' then 3
                when split_part('AX_FSCL_CLNDR', '_', 1) = 'JDE' then 1
                when split_part('AX_FSCL_CLNDR', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_FSCL_CLNDR', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_FSCL_CLNDR', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_FSCL_CLNDR', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_FSCL_CLNDR', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_FSCL_CLNDR
        
         union all 
    
        select 
        'AX_OPCO_ASSCTN' as model_nm,
        replace('AX_OPCO_ASSCTN', concat(split_part('AX_OPCO_ASSCTN', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_ASSCTN', concat(split_part('AX_OPCO_ASSCTN', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_ASSCTN', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_ASSCTN', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_ASSCTN', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_ASSCTN', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_ASSCTN', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_ASSCTN', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_ASSCTN', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_ASSCTN', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_ASSCTN
        
         union all 
    
        select 
        'AX_OPCO_COST_CENTER' as model_nm,
        replace('AX_OPCO_COST_CENTER', concat(split_part('AX_OPCO_COST_CENTER', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_COST_CENTER', concat(split_part('AX_OPCO_COST_CENTER', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_COST_CENTER', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_COST_CENTER', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_COST_CENTER', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_COST_CENTER', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_COST_CENTER', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_COST_CENTER', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_COST_CENTER', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_COST_CENTER', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_COST_CENTER
        
         union all 
    
        select 
        'AX_OPCO_DEPT' as model_nm,
        replace('AX_OPCO_DEPT', concat(split_part('AX_OPCO_DEPT', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_DEPT', concat(split_part('AX_OPCO_DEPT', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_DEPT', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_DEPT', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_DEPT', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_DEPT', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_DEPT', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_DEPT', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_DEPT', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_DEPT', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_DEPT
        
         union all 
    
        select 
        'AX_OPCO_SITE' as model_nm,
        replace('AX_OPCO_SITE', concat(split_part('AX_OPCO_SITE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_SITE', concat(split_part('AX_OPCO_SITE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_SITE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_SITE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_SITE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_SITE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_SITE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_SITE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_SITE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_SITE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_SITE
        
         union all 
    
        select 
        'AX_OPCO_TYPE' as model_nm,
        replace('AX_OPCO_TYPE', concat(split_part('AX_OPCO_TYPE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_TYPE', concat(split_part('AX_OPCO_TYPE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_TYPE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_TYPE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_TYPE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_TYPE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_TYPE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_TYPE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_TYPE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_TYPE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_TYPE
        
         union all 
    
        select 
        'AX_OPCO' as model_nm,
        replace('AX_OPCO', concat(split_part('AX_OPCO', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO', concat(split_part('AX_OPCO', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO
        
         union all 
    
        select 
        'AX_REGION' as model_nm,
        replace('AX_REGION', concat(split_part('AX_REGION', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_REGION', concat(split_part('AX_REGION', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_REGION', '_', 1) = 'AX' then 3
                when split_part('AX_REGION', '_', 1) = 'NS' then 3
                when split_part('AX_REGION', '_', 1) = 'JDE' then 1
                when split_part('AX_REGION', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_REGION', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_REGION', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_REGION', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_REGION', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_REGION
        
         union all 
    
        select 
        'AX_SUB_REGION' as model_nm,
        replace('AX_SUB_REGION', concat(split_part('AX_SUB_REGION', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_SUB_REGION', concat(split_part('AX_SUB_REGION', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_SUB_REGION', '_', 1) = 'AX' then 3
                when split_part('AX_SUB_REGION', '_', 1) = 'NS' then 3
                when split_part('AX_SUB_REGION', '_', 1) = 'JDE' then 1
                when split_part('AX_SUB_REGION', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_SUB_REGION', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_SUB_REGION', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_SUB_REGION', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_SUB_REGION', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_SUB_REGION
        
         union all 
    
        select 
        'AX_WAREHOUSE' as model_nm,
        replace('AX_WAREHOUSE', concat(split_part('AX_WAREHOUSE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_WAREHOUSE', concat(split_part('AX_WAREHOUSE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_WAREHOUSE', '_', 1) = 'AX' then 3
                when split_part('AX_WAREHOUSE', '_', 1) = 'NS' then 3
                when split_part('AX_WAREHOUSE', '_', 1) = 'JDE' then 1
                when split_part('AX_WAREHOUSE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_WAREHOUSE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_WAREHOUSE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_WAREHOUSE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_WAREHOUSE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_WAREHOUSE
        
         union all 
    
        select 
        'AX_OPCO_INVTRY_TRANS' as model_nm,
        replace('AX_OPCO_INVTRY_TRANS', concat(split_part('AX_OPCO_INVTRY_TRANS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_INVTRY_TRANS', concat(split_part('AX_OPCO_INVTRY_TRANS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_INVTRY_TRANS
        
         union all 
    
        select 
        'AX_OPCO_ITEM_CLASS' as model_nm,
        replace('AX_OPCO_ITEM_CLASS', concat(split_part('AX_OPCO_ITEM_CLASS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_ITEM_CLASS', concat(split_part('AX_OPCO_ITEM_CLASS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_ITEM_CLASS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_ITEM_CLASS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_ITEM_CLASS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_ITEM_CLASS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_ITEM_CLASS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_ITEM_CLASS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_ITEM_CLASS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_ITEM_CLASS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_ITEM_CLASS
        
         union all 
    
        select 
        'AX_OPCO_INVTRY_TRANS' as model_nm,
        replace('AX_OPCO_INVTRY_TRANS', concat(split_part('AX_OPCO_INVTRY_TRANS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_INVTRY_TRANS', concat(split_part('AX_OPCO_INVTRY_TRANS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_INVTRY_TRANS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_INVTRY_TRANS
        
         union all 
    
        select 
        'AX_OPCO_ITEM_CVRG_GRP' as model_nm,
        replace('AX_OPCO_ITEM_CVRG_GRP', concat(split_part('AX_OPCO_ITEM_CVRG_GRP', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_ITEM_CVRG_GRP', concat(split_part('AX_OPCO_ITEM_CVRG_GRP', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_ITEM_CVRG_GRP', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_ITEM_CVRG_GRP', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_ITEM_CVRG_GRP', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_ITEM_CVRG_GRP', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_ITEM_CVRG_GRP', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_ITEM_CVRG_GRP', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_ITEM_CVRG_GRP', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_ITEM_CVRG_GRP', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_ITEM_CVRG_GRP
        
         union all 
    
        select 
        'AX_OPCO_ITEM_FREIGHT' as model_nm,
        replace('AX_OPCO_ITEM_FREIGHT', concat(split_part('AX_OPCO_ITEM_FREIGHT', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_ITEM_FREIGHT', concat(split_part('AX_OPCO_ITEM_FREIGHT', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_ITEM_FREIGHT', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_ITEM_FREIGHT', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_ITEM_FREIGHT', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_ITEM_FREIGHT', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_ITEM_FREIGHT', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_ITEM_FREIGHT', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_ITEM_FREIGHT', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_ITEM_FREIGHT', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_ITEM_FREIGHT
        
         union all 
    
        select 
        'AX_OPCO_ITEM_GRP' as model_nm,
        replace('AX_OPCO_ITEM_GRP', concat(split_part('AX_OPCO_ITEM_GRP', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_ITEM_GRP', concat(split_part('AX_OPCO_ITEM_GRP', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_ITEM_GRP', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_ITEM_GRP', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_ITEM_GRP', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_ITEM_GRP', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_ITEM_GRP', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_ITEM_GRP', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_ITEM_GRP', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_ITEM_GRP', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_ITEM_GRP
        
         union all 
    
        select 
        'AX_OPCO_ITEM_INVTRY' as model_nm,
        replace('AX_OPCO_ITEM_INVTRY', concat(split_part('AX_OPCO_ITEM_INVTRY', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_ITEM_INVTRY', concat(split_part('AX_OPCO_ITEM_INVTRY', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_ITEM_INVTRY', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_ITEM_INVTRY', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_ITEM_INVTRY', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_ITEM_INVTRY', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_ITEM_INVTRY', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_ITEM_INVTRY', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_ITEM_INVTRY', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_ITEM_INVTRY', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_ITEM_INVTRY
        
         union all 
    
        select 
        'AX_OPCO_ITEM_MODEL_GRP' as model_nm,
        replace('AX_OPCO_ITEM_MODEL_GRP', concat(split_part('AX_OPCO_ITEM_MODEL_GRP', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_ITEM_MODEL_GRP', concat(split_part('AX_OPCO_ITEM_MODEL_GRP', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_ITEM_MODEL_GRP', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_ITEM_MODEL_GRP', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_ITEM_MODEL_GRP', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_ITEM_MODEL_GRP', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_ITEM_MODEL_GRP', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_ITEM_MODEL_GRP', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_ITEM_MODEL_GRP', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_ITEM_MODEL_GRP', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_ITEM_MODEL_GRP
        
         union all 
    
        select 
        'AX_OPCO_ITEM_OPCO_ASSCTN_XREF' as model_nm,
        replace('AX_OPCO_ITEM_OPCO_ASSCTN_XREF', concat(split_part('AX_OPCO_ITEM_OPCO_ASSCTN_XREF', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_ITEM_OPCO_ASSCTN_XREF', concat(split_part('AX_OPCO_ITEM_OPCO_ASSCTN_XREF', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_ITEM_OPCO_ASSCTN_XREF', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_ITEM_OPCO_ASSCTN_XREF', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_ITEM_OPCO_ASSCTN_XREF', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_ITEM_OPCO_ASSCTN_XREF', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_ITEM_OPCO_ASSCTN_XREF', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_ITEM_OPCO_ASSCTN_XREF', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_ITEM_OPCO_ASSCTN_XREF', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_ITEM_OPCO_ASSCTN_XREF', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_ITEM_OPCO_ASSCTN_XREF
        
         union all 
    
        select 
        'AX_OPCO_ITEM_PSI_DTL' as model_nm,
        replace('AX_OPCO_ITEM_PSI_DTL', concat(split_part('AX_OPCO_ITEM_PSI_DTL', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_ITEM_PSI_DTL', concat(split_part('AX_OPCO_ITEM_PSI_DTL', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_ITEM_PSI_DTL', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_ITEM_PSI_DTL', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_ITEM_PSI_DTL', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_ITEM_PSI_DTL', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_ITEM_PSI_DTL', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_ITEM_PSI_DTL', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_ITEM_PSI_DTL', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_ITEM_PSI_DTL', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_ITEM_PSI_DTL
        
         union all 
    
        select 
        'AX_OPCO_ITEM_SIZE' as model_nm,
        replace('AX_OPCO_ITEM_SIZE', concat(split_part('AX_OPCO_ITEM_SIZE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_ITEM_SIZE', concat(split_part('AX_OPCO_ITEM_SIZE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_ITEM_SIZE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_ITEM_SIZE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_ITEM_SIZE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_ITEM_SIZE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_ITEM_SIZE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_ITEM_SIZE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_ITEM_SIZE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_ITEM_SIZE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_ITEM_SIZE
        
         union all 
    
        select 
        'AX_OPCO_ITEM_SLS_CLASS' as model_nm,
        replace('AX_OPCO_ITEM_SLS_CLASS', concat(split_part('AX_OPCO_ITEM_SLS_CLASS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_ITEM_SLS_CLASS', concat(split_part('AX_OPCO_ITEM_SLS_CLASS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_ITEM_SLS_CLASS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_ITEM_SLS_CLASS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_ITEM_SLS_CLASS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_ITEM_SLS_CLASS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_ITEM_SLS_CLASS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_ITEM_SLS_CLASS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_ITEM_SLS_CLASS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_ITEM_SLS_CLASS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_ITEM_SLS_CLASS
        
         union all 
    
        select 
        'AX_OPCO_ITEM_SUBTYPE' as model_nm,
        replace('AX_OPCO_ITEM_SUBTYPE', concat(split_part('AX_OPCO_ITEM_SUBTYPE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_ITEM_SUBTYPE', concat(split_part('AX_OPCO_ITEM_SUBTYPE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_ITEM_SUBTYPE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_ITEM_SUBTYPE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_ITEM_SUBTYPE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_ITEM_SUBTYPE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_ITEM_SUBTYPE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_ITEM_SUBTYPE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_ITEM_SUBTYPE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_ITEM_SUBTYPE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_ITEM_SUBTYPE
        
         union all 
    
        select 
        'AX_OPCO_ITEM_TYPE' as model_nm,
        replace('AX_OPCO_ITEM_TYPE', concat(split_part('AX_OPCO_ITEM_TYPE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_ITEM_TYPE', concat(split_part('AX_OPCO_ITEM_TYPE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_ITEM_TYPE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_ITEM_TYPE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_ITEM_TYPE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_ITEM_TYPE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_ITEM_TYPE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_ITEM_TYPE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_ITEM_TYPE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_ITEM_TYPE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_ITEM_TYPE
        
         union all 
    
        select 
        'AX_OPCO_ITEM' as model_nm,
        replace('AX_OPCO_ITEM', concat(split_part('AX_OPCO_ITEM', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_ITEM', concat(split_part('AX_OPCO_ITEM', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_ITEM', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_ITEM', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_ITEM', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_ITEM', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_ITEM', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_ITEM', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_ITEM', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_ITEM', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_ITEM
        
         union all 
    
        select 
        'AX_OPCO_LOB' as model_nm,
        replace('AX_OPCO_LOB', concat(split_part('AX_OPCO_LOB', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_LOB', concat(split_part('AX_OPCO_LOB', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_LOB', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_LOB', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_LOB', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_LOB', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_LOB', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_LOB', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_LOB', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_LOB', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_LOB
        
         union all 
    
        select 
        'AX_OPCO_PURPOSE' as model_nm,
        replace('AX_OPCO_PURPOSE', concat(split_part('AX_OPCO_PURPOSE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PURPOSE', concat(split_part('AX_OPCO_PURPOSE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PURPOSE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PURPOSE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PURPOSE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PURPOSE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PURPOSE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PURPOSE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PURPOSE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PURPOSE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PURPOSE
        
         union all 
    
        select 
        'AX_OPCO_UOM' as model_nm,
        replace('AX_OPCO_UOM', concat(split_part('AX_OPCO_UOM', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_UOM', concat(split_part('AX_OPCO_UOM', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_UOM', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_UOM', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_UOM', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_UOM', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_UOM', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_UOM', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_UOM', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_UOM', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_UOM
        
         union all 
    
        select 
        'AX_OPCO_VENDOR' as model_nm,
        replace('AX_OPCO_VENDOR', concat(split_part('AX_OPCO_VENDOR', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_VENDOR', concat(split_part('AX_OPCO_VENDOR', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_VENDOR', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_VENDOR', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_VENDOR', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_VENDOR', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_VENDOR', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_VENDOR', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_VENDOR', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_VENDOR', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_VENDOR
        
         union all 
    
        select 
        'AX_OPCO_PROJECT_CATGRY' as model_nm,
        replace('AX_OPCO_PROJECT_CATGRY', concat(split_part('AX_OPCO_PROJECT_CATGRY', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PROJECT_CATGRY', concat(split_part('AX_OPCO_PROJECT_CATGRY', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PROJECT_CATGRY', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PROJECT_CATGRY', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PROJECT_CATGRY', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PROJECT_CATGRY', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PROJECT_CATGRY', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_CATGRY', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_CATGRY', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PROJECT_CATGRY', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PROJECT_CATGRY
        
         union all 
    
        select 
        'AX_OPCO_PROJECT_COST_FRCST' as model_nm,
        replace('AX_OPCO_PROJECT_COST_FRCST', concat(split_part('AX_OPCO_PROJECT_COST_FRCST', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PROJECT_COST_FRCST', concat(split_part('AX_OPCO_PROJECT_COST_FRCST', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PROJECT_COST_FRCST', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PROJECT_COST_FRCST', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PROJECT_COST_FRCST', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PROJECT_COST_FRCST', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PROJECT_COST_FRCST', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_COST_FRCST', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_COST_FRCST', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PROJECT_COST_FRCST', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PROJECT_COST_FRCST
        
         union all 
    
        select 
        'AX_OPCO_PROJECT_COST_TRANS' as model_nm,
        replace('AX_OPCO_PROJECT_COST_TRANS', concat(split_part('AX_OPCO_PROJECT_COST_TRANS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PROJECT_COST_TRANS', concat(split_part('AX_OPCO_PROJECT_COST_TRANS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PROJECT_COST_TRANS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PROJECT_COST_TRANS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PROJECT_COST_TRANS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PROJECT_COST_TRANS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PROJECT_COST_TRANS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_COST_TRANS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_COST_TRANS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PROJECT_COST_TRANS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PROJECT_COST_TRANS
        
         union all 
    
        select 
        'AX_OPCO_PROJECT_EMPL_FRCST' as model_nm,
        replace('AX_OPCO_PROJECT_EMPL_FRCST', concat(split_part('AX_OPCO_PROJECT_EMPL_FRCST', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PROJECT_EMPL_FRCST', concat(split_part('AX_OPCO_PROJECT_EMPL_FRCST', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PROJECT_EMPL_FRCST', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PROJECT_EMPL_FRCST', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PROJECT_EMPL_FRCST', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PROJECT_EMPL_FRCST', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PROJECT_EMPL_FRCST', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_EMPL_FRCST', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_EMPL_FRCST', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PROJECT_EMPL_FRCST', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PROJECT_EMPL_FRCST
        
         union all 
    
        select 
        'AX_OPCO_PROJECT_EMPL_TRANS' as model_nm,
        replace('AX_OPCO_PROJECT_EMPL_TRANS', concat(split_part('AX_OPCO_PROJECT_EMPL_TRANS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PROJECT_EMPL_TRANS', concat(split_part('AX_OPCO_PROJECT_EMPL_TRANS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PROJECT_EMPL_TRANS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PROJECT_EMPL_TRANS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PROJECT_EMPL_TRANS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PROJECT_EMPL_TRANS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PROJECT_EMPL_TRANS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_EMPL_TRANS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_EMPL_TRANS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PROJECT_EMPL_TRANS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PROJECT_EMPL_TRANS
        
         union all 
    
        select 
        'AX_OPCO_PROJECT_INVOICE_PROJECT_XREF' as model_nm,
        replace('AX_OPCO_PROJECT_INVOICE_PROJECT_XREF', concat(split_part('AX_OPCO_PROJECT_INVOICE_PROJECT_XREF', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PROJECT_INVOICE_PROJECT_XREF', concat(split_part('AX_OPCO_PROJECT_INVOICE_PROJECT_XREF', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PROJECT_INVOICE_PROJECT_XREF', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PROJECT_INVOICE_PROJECT_XREF', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PROJECT_INVOICE_PROJECT_XREF', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PROJECT_INVOICE_PROJECT_XREF', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PROJECT_INVOICE_PROJECT_XREF', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_INVOICE_PROJECT_XREF', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_INVOICE_PROJECT_XREF', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PROJECT_INVOICE_PROJECT_XREF', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PROJECT_INVOICE_PROJECT_XREF
        
         union all 
    
        select 
        'AX_OPCO_PROJECT_ITEM_TRANS' as model_nm,
        replace('AX_OPCO_PROJECT_ITEM_TRANS', concat(split_part('AX_OPCO_PROJECT_ITEM_TRANS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PROJECT_ITEM_TRANS', concat(split_part('AX_OPCO_PROJECT_ITEM_TRANS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PROJECT_ITEM_TRANS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PROJECT_ITEM_TRANS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PROJECT_ITEM_TRANS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PROJECT_ITEM_TRANS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PROJECT_ITEM_TRANS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_ITEM_TRANS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_ITEM_TRANS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PROJECT_ITEM_TRANS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PROJECT_ITEM_TRANS
        
         union all 
    
        select 
        'AX_OPCO_PROJECT_REVENUE_TRANS' as model_nm,
        replace('AX_OPCO_PROJECT_REVENUE_TRANS', concat(split_part('AX_OPCO_PROJECT_REVENUE_TRANS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PROJECT_REVENUE_TRANS', concat(split_part('AX_OPCO_PROJECT_REVENUE_TRANS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PROJECT_REVENUE_TRANS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PROJECT_REVENUE_TRANS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PROJECT_REVENUE_TRANS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PROJECT_REVENUE_TRANS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PROJECT_REVENUE_TRANS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_REVENUE_TRANS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_REVENUE_TRANS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PROJECT_REVENUE_TRANS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PROJECT_REVENUE_TRANS
        
         union all 
    
        select 
        'AX_OPCO_PROJECT_TRANS_POSTING' as model_nm,
        replace('AX_OPCO_PROJECT_TRANS_POSTING', concat(split_part('AX_OPCO_PROJECT_TRANS_POSTING', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PROJECT_TRANS_POSTING', concat(split_part('AX_OPCO_PROJECT_TRANS_POSTING', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PROJECT_TRANS_POSTING', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PROJECT_TRANS_POSTING', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PROJECT_TRANS_POSTING', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PROJECT_TRANS_POSTING', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PROJECT_TRANS_POSTING', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_TRANS_POSTING', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PROJECT_TRANS_POSTING', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PROJECT_TRANS_POSTING', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PROJECT_TRANS_POSTING
        
         union all 
    
        select 
        'AX_OPCO_PROJECT' as model_nm,
        replace('AX_OPCO_PROJECT', concat(split_part('AX_OPCO_PROJECT', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PROJECT', concat(split_part('AX_OPCO_PROJECT', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PROJECT', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PROJECT', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PROJECT', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PROJECT', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PROJECT', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PROJECT', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PROJECT', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PROJECT', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PROJECT
        
         union all 
    
        select 
        'AX_OPCO_PO_LINE' as model_nm,
        replace('AX_OPCO_PO_LINE', concat(split_part('AX_OPCO_PO_LINE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PO_LINE', concat(split_part('AX_OPCO_PO_LINE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PO_LINE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PO_LINE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PO_LINE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PO_LINE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PO_LINE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PO_LINE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PO_LINE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PO_LINE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PO_LINE
        
         union all 
    
        select 
        'AX_OPCO_PO' as model_nm,
        replace('AX_OPCO_PO', concat(split_part('AX_OPCO_PO', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_PO', concat(split_part('AX_OPCO_PO', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_PO', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_PO', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_PO', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_PO', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_PO', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_PO', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_PO', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_PO', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_PO
        
         union all 
    
        select 
        'AX_OPCO_DLVRY_MODE' as model_nm,
        replace('AX_OPCO_DLVRY_MODE', concat(split_part('AX_OPCO_DLVRY_MODE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_DLVRY_MODE', concat(split_part('AX_OPCO_DLVRY_MODE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_DLVRY_MODE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_DLVRY_MODE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_DLVRY_MODE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_DLVRY_MODE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_DLVRY_MODE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_DLVRY_MODE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_DLVRY_MODE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_DLVRY_MODE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_DLVRY_MODE
        
         union all 
    
        select 
        'AX_OPCO_DLVRY_TERMS' as model_nm,
        replace('AX_OPCO_DLVRY_TERMS', concat(split_part('AX_OPCO_DLVRY_TERMS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_DLVRY_TERMS', concat(split_part('AX_OPCO_DLVRY_TERMS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_DLVRY_TERMS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_DLVRY_TERMS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_DLVRY_TERMS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_DLVRY_TERMS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_DLVRY_TERMS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_DLVRY_TERMS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_DLVRY_TERMS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_DLVRY_TERMS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_DLVRY_TERMS
        
         union all 
    
        select 
        'AX_OPCO_SLS_ORDR_LINE' as model_nm,
        replace('AX_OPCO_SLS_ORDR_LINE', concat(split_part('AX_OPCO_SLS_ORDR_LINE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_SLS_ORDR_LINE', concat(split_part('AX_OPCO_SLS_ORDR_LINE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_SLS_ORDR_LINE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_SLS_ORDR_LINE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_SLS_ORDR_LINE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_SLS_ORDR_LINE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_SLS_ORDR_LINE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_SLS_ORDR_LINE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_SLS_ORDR_LINE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_SLS_ORDR_LINE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_SLS_ORDR_LINE
        
         union all 
    
        select 
        'AX_OPCO_SLS_ORDR' as model_nm,
        replace('AX_OPCO_SLS_ORDR', concat(split_part('AX_OPCO_SLS_ORDR', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_SLS_ORDR', concat(split_part('AX_OPCO_SLS_ORDR', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_SLS_ORDR', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_SLS_ORDR', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_SLS_ORDR', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_SLS_ORDR', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_SLS_ORDR', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_SLS_ORDR', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_SLS_ORDR', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_SLS_ORDR', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_SLS_ORDR
        
         union all 
    
        select 
        'AX_SLS_ORDR_OPCO_LOCN_XREF' as model_nm,
        replace('AX_SLS_ORDR_OPCO_LOCN_XREF', concat(split_part('AX_SLS_ORDR_OPCO_LOCN_XREF', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_SLS_ORDR_OPCO_LOCN_XREF', concat(split_part('AX_SLS_ORDR_OPCO_LOCN_XREF', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_SLS_ORDR_OPCO_LOCN_XREF', '_', 1) = 'AX' then 3
                when split_part('AX_SLS_ORDR_OPCO_LOCN_XREF', '_', 1) = 'NS' then 3
                when split_part('AX_SLS_ORDR_OPCO_LOCN_XREF', '_', 1) = 'JDE' then 1
                when split_part('AX_SLS_ORDR_OPCO_LOCN_XREF', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_SLS_ORDR_OPCO_LOCN_XREF', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_SLS_ORDR_OPCO_LOCN_XREF', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_SLS_ORDR_OPCO_LOCN_XREF', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_SLS_ORDR_OPCO_LOCN_XREF', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_SLS_ORDR_OPCO_LOCN_XREF
        
         union all 
    
        select 
        'AX_OPCO_VENDOR_INVOICE_LINE' as model_nm,
        replace('AX_OPCO_VENDOR_INVOICE_LINE', concat(split_part('AX_OPCO_VENDOR_INVOICE_LINE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_VENDOR_INVOICE_LINE', concat(split_part('AX_OPCO_VENDOR_INVOICE_LINE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_VENDOR_INVOICE_LINE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_VENDOR_INVOICE_LINE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_VENDOR_INVOICE_LINE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_VENDOR_INVOICE_LINE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_VENDOR_INVOICE_LINE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_VENDOR_INVOICE_LINE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_VENDOR_INVOICE_LINE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_VENDOR_INVOICE_LINE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_VENDOR_INVOICE_LINE
        
         union all 
    
        select 
        'AX_OPCO_VENDOR_INVOICE' as model_nm,
        replace('AX_OPCO_VENDOR_INVOICE', concat(split_part('AX_OPCO_VENDOR_INVOICE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_VENDOR_INVOICE', concat(split_part('AX_OPCO_VENDOR_INVOICE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_VENDOR_INVOICE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_VENDOR_INVOICE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_VENDOR_INVOICE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_VENDOR_INVOICE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_VENDOR_INVOICE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_VENDOR_INVOICE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_VENDOR_INVOICE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_VENDOR_INVOICE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_VENDOR_INVOICE
        
         union all 
    
        select 
        'AX_OPCO_VENDOR_PACKING_SLIP_LINE' as model_nm,
        replace('AX_OPCO_VENDOR_PACKING_SLIP_LINE', concat(split_part('AX_OPCO_VENDOR_PACKING_SLIP_LINE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_VENDOR_PACKING_SLIP_LINE', concat(split_part('AX_OPCO_VENDOR_PACKING_SLIP_LINE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP_LINE', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP_LINE', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP_LINE', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP_LINE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP_LINE', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP_LINE', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP_LINE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP_LINE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_VENDOR_PACKING_SLIP_LINE
        
         union all 
    
        select 
        'AX_OPCO_VENDOR_PACKING_SLIP' as model_nm,
        replace('AX_OPCO_VENDOR_PACKING_SLIP', concat(split_part('AX_OPCO_VENDOR_PACKING_SLIP', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_VENDOR_PACKING_SLIP', concat(split_part('AX_OPCO_VENDOR_PACKING_SLIP', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_VENDOR_PACKING_SLIP', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_VENDOR_PACKING_SLIP
        
         union all 
    
        select 
        'AX_OPCO_VENDOR_TRANS' as model_nm,
        replace('AX_OPCO_VENDOR_TRANS', concat(split_part('AX_OPCO_VENDOR_TRANS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('AX_OPCO_VENDOR_TRANS', concat(split_part('AX_OPCO_VENDOR_TRANS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('AX_OPCO_VENDOR_TRANS', '_', 1) = 'AX' then 3
                when split_part('AX_OPCO_VENDOR_TRANS', '_', 1) = 'NS' then 3
                when split_part('AX_OPCO_VENDOR_TRANS', '_', 1) = 'JDE' then 1
                when split_part('AX_OPCO_VENDOR_TRANS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('AX_OPCO_VENDOR_TRANS', '_', 1) = 'AX' then 'Incremental'
                when split_part('AX_OPCO_VENDOR_TRANS', '_', 1) = 'NS' then 'Incremental'
                when split_part('AX_OPCO_VENDOR_TRANS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('AX_OPCO_VENDOR_TRANS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.AX_OPCO_VENDOR_TRANS
        
         union all 
    
        select 
        'NS_OPCO_CURRENCY' as model_nm,
        replace('NS_OPCO_CURRENCY', concat(split_part('NS_OPCO_CURRENCY', '_', 1), '_')) as target_model_nm,
        case 
            when replace('NS_OPCO_CURRENCY', concat(split_part('NS_OPCO_CURRENCY', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('NS_OPCO_CURRENCY', '_', 1) = 'AX' then 3
                when split_part('NS_OPCO_CURRENCY', '_', 1) = 'NS' then 3
                when split_part('NS_OPCO_CURRENCY', '_', 1) = 'JDE' then 1
                when split_part('NS_OPCO_CURRENCY', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('NS_OPCO_CURRENCY', '_', 1) = 'AX' then 'Incremental'
                when split_part('NS_OPCO_CURRENCY', '_', 1) = 'NS' then 'Incremental'
                when split_part('NS_OPCO_CURRENCY', '_', 1) = 'JDE' then 'Full Load'
                when split_part('NS_OPCO_CURRENCY', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.NS_OPCO_CURRENCY
        
         union all 
    
        select 
        'NS_OPCO_PYMNT_MODE' as model_nm,
        replace('NS_OPCO_PYMNT_MODE', concat(split_part('NS_OPCO_PYMNT_MODE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('NS_OPCO_PYMNT_MODE', concat(split_part('NS_OPCO_PYMNT_MODE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('NS_OPCO_PYMNT_MODE', '_', 1) = 'AX' then 3
                when split_part('NS_OPCO_PYMNT_MODE', '_', 1) = 'NS' then 3
                when split_part('NS_OPCO_PYMNT_MODE', '_', 1) = 'JDE' then 1
                when split_part('NS_OPCO_PYMNT_MODE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('NS_OPCO_PYMNT_MODE', '_', 1) = 'AX' then 'Incremental'
                when split_part('NS_OPCO_PYMNT_MODE', '_', 1) = 'NS' then 'Incremental'
                when split_part('NS_OPCO_PYMNT_MODE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('NS_OPCO_PYMNT_MODE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.NS_OPCO_PYMNT_MODE
        
         union all 
    
        select 
        'NS_OPCO_PYMNT_TERMS' as model_nm,
        replace('NS_OPCO_PYMNT_TERMS', concat(split_part('NS_OPCO_PYMNT_TERMS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('NS_OPCO_PYMNT_TERMS', concat(split_part('NS_OPCO_PYMNT_TERMS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('NS_OPCO_PYMNT_TERMS', '_', 1) = 'AX' then 3
                when split_part('NS_OPCO_PYMNT_TERMS', '_', 1) = 'NS' then 3
                when split_part('NS_OPCO_PYMNT_TERMS', '_', 1) = 'JDE' then 1
                when split_part('NS_OPCO_PYMNT_TERMS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('NS_OPCO_PYMNT_TERMS', '_', 1) = 'AX' then 'Incremental'
                when split_part('NS_OPCO_PYMNT_TERMS', '_', 1) = 'NS' then 'Incremental'
                when split_part('NS_OPCO_PYMNT_TERMS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('NS_OPCO_PYMNT_TERMS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.NS_OPCO_PYMNT_TERMS
        
         union all 
    
        select 
        'NS_OPCO_CHART_OF_ACCTS' as model_nm,
        replace('NS_OPCO_CHART_OF_ACCTS', concat(split_part('NS_OPCO_CHART_OF_ACCTS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('NS_OPCO_CHART_OF_ACCTS', concat(split_part('NS_OPCO_CHART_OF_ACCTS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('NS_OPCO_CHART_OF_ACCTS', '_', 1) = 'AX' then 3
                when split_part('NS_OPCO_CHART_OF_ACCTS', '_', 1) = 'NS' then 3
                when split_part('NS_OPCO_CHART_OF_ACCTS', '_', 1) = 'JDE' then 1
                when split_part('NS_OPCO_CHART_OF_ACCTS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('NS_OPCO_CHART_OF_ACCTS', '_', 1) = 'AX' then 'Incremental'
                when split_part('NS_OPCO_CHART_OF_ACCTS', '_', 1) = 'NS' then 'Incremental'
                when split_part('NS_OPCO_CHART_OF_ACCTS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('NS_OPCO_CHART_OF_ACCTS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.NS_OPCO_CHART_OF_ACCTS
        
         union all 
    
        select 
        'NS_OPCO_FSCL_YR_GL_OPENING_BAL' as model_nm,
        replace('NS_OPCO_FSCL_YR_GL_OPENING_BAL', concat(split_part('NS_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1), '_')) as target_model_nm,
        case 
            when replace('NS_OPCO_FSCL_YR_GL_OPENING_BAL', concat(split_part('NS_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('NS_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'AX' then 3
                when split_part('NS_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'NS' then 3
                when split_part('NS_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'JDE' then 1
                when split_part('NS_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('NS_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'AX' then 'Incremental'
                when split_part('NS_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'NS' then 'Incremental'
                when split_part('NS_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'JDE' then 'Full Load'
                when split_part('NS_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.NS_OPCO_FSCL_YR_GL_OPENING_BAL
        
         union all 
    
        select 
        'NS_OPCO_GL_TRANS' as model_nm,
        replace('NS_OPCO_GL_TRANS', concat(split_part('NS_OPCO_GL_TRANS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('NS_OPCO_GL_TRANS', concat(split_part('NS_OPCO_GL_TRANS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('NS_OPCO_GL_TRANS', '_', 1) = 'AX' then 3
                when split_part('NS_OPCO_GL_TRANS', '_', 1) = 'NS' then 3
                when split_part('NS_OPCO_GL_TRANS', '_', 1) = 'JDE' then 1
                when split_part('NS_OPCO_GL_TRANS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('NS_OPCO_GL_TRANS', '_', 1) = 'AX' then 'Incremental'
                when split_part('NS_OPCO_GL_TRANS', '_', 1) = 'NS' then 'Incremental'
                when split_part('NS_OPCO_GL_TRANS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('NS_OPCO_GL_TRANS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.NS_OPCO_GL_TRANS
        
         union all 
    
        select 
        'NS_OPCO_COST_CENTER' as model_nm,
        replace('NS_OPCO_COST_CENTER', concat(split_part('NS_OPCO_COST_CENTER', '_', 1), '_')) as target_model_nm,
        case 
            when replace('NS_OPCO_COST_CENTER', concat(split_part('NS_OPCO_COST_CENTER', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('NS_OPCO_COST_CENTER', '_', 1) = 'AX' then 3
                when split_part('NS_OPCO_COST_CENTER', '_', 1) = 'NS' then 3
                when split_part('NS_OPCO_COST_CENTER', '_', 1) = 'JDE' then 1
                when split_part('NS_OPCO_COST_CENTER', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('NS_OPCO_COST_CENTER', '_', 1) = 'AX' then 'Incremental'
                when split_part('NS_OPCO_COST_CENTER', '_', 1) = 'NS' then 'Incremental'
                when split_part('NS_OPCO_COST_CENTER', '_', 1) = 'JDE' then 'Full Load'
                when split_part('NS_OPCO_COST_CENTER', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.NS_OPCO_COST_CENTER
        
         union all 
    
        select 
        'NS_OPCO_DEPT' as model_nm,
        replace('NS_OPCO_DEPT', concat(split_part('NS_OPCO_DEPT', '_', 1), '_')) as target_model_nm,
        case 
            when replace('NS_OPCO_DEPT', concat(split_part('NS_OPCO_DEPT', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('NS_OPCO_DEPT', '_', 1) = 'AX' then 3
                when split_part('NS_OPCO_DEPT', '_', 1) = 'NS' then 3
                when split_part('NS_OPCO_DEPT', '_', 1) = 'JDE' then 1
                when split_part('NS_OPCO_DEPT', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('NS_OPCO_DEPT', '_', 1) = 'AX' then 'Incremental'
                when split_part('NS_OPCO_DEPT', '_', 1) = 'NS' then 'Incremental'
                when split_part('NS_OPCO_DEPT', '_', 1) = 'JDE' then 'Full Load'
                when split_part('NS_OPCO_DEPT', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.NS_OPCO_DEPT
        
         union all 
    
        select 
        'NS_OPCO_TYPE' as model_nm,
        replace('NS_OPCO_TYPE', concat(split_part('NS_OPCO_TYPE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('NS_OPCO_TYPE', concat(split_part('NS_OPCO_TYPE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('NS_OPCO_TYPE', '_', 1) = 'AX' then 3
                when split_part('NS_OPCO_TYPE', '_', 1) = 'NS' then 3
                when split_part('NS_OPCO_TYPE', '_', 1) = 'JDE' then 1
                when split_part('NS_OPCO_TYPE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('NS_OPCO_TYPE', '_', 1) = 'AX' then 'Incremental'
                when split_part('NS_OPCO_TYPE', '_', 1) = 'NS' then 'Incremental'
                when split_part('NS_OPCO_TYPE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('NS_OPCO_TYPE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.NS_OPCO_TYPE
        
         union all 
    
        select 
        'NS_OPCO' as model_nm,
        replace('NS_OPCO', concat(split_part('NS_OPCO', '_', 1), '_')) as target_model_nm,
        case 
            when replace('NS_OPCO', concat(split_part('NS_OPCO', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('NS_OPCO', '_', 1) = 'AX' then 3
                when split_part('NS_OPCO', '_', 1) = 'NS' then 3
                when split_part('NS_OPCO', '_', 1) = 'JDE' then 1
                when split_part('NS_OPCO', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('NS_OPCO', '_', 1) = 'AX' then 'Incremental'
                when split_part('NS_OPCO', '_', 1) = 'NS' then 'Incremental'
                when split_part('NS_OPCO', '_', 1) = 'JDE' then 'Full Load'
                when split_part('NS_OPCO', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.NS_OPCO
        
         union all 
    
        select 
        'NS_REGION' as model_nm,
        replace('NS_REGION', concat(split_part('NS_REGION', '_', 1), '_')) as target_model_nm,
        case 
            when replace('NS_REGION', concat(split_part('NS_REGION', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('NS_REGION', '_', 1) = 'AX' then 3
                when split_part('NS_REGION', '_', 1) = 'NS' then 3
                when split_part('NS_REGION', '_', 1) = 'JDE' then 1
                when split_part('NS_REGION', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('NS_REGION', '_', 1) = 'AX' then 'Incremental'
                when split_part('NS_REGION', '_', 1) = 'NS' then 'Incremental'
                when split_part('NS_REGION', '_', 1) = 'JDE' then 'Full Load'
                when split_part('NS_REGION', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.NS_REGION
        
         union all 
    
        select 
        'NS_SUB_REGION' as model_nm,
        replace('NS_SUB_REGION', concat(split_part('NS_SUB_REGION', '_', 1), '_')) as target_model_nm,
        case 
            when replace('NS_SUB_REGION', concat(split_part('NS_SUB_REGION', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('NS_SUB_REGION', '_', 1) = 'AX' then 3
                when split_part('NS_SUB_REGION', '_', 1) = 'NS' then 3
                when split_part('NS_SUB_REGION', '_', 1) = 'JDE' then 1
                when split_part('NS_SUB_REGION', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('NS_SUB_REGION', '_', 1) = 'AX' then 'Incremental'
                when split_part('NS_SUB_REGION', '_', 1) = 'NS' then 'Incremental'
                when split_part('NS_SUB_REGION', '_', 1) = 'JDE' then 'Full Load'
                when split_part('NS_SUB_REGION', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.NS_SUB_REGION
        
         union all 
    
        select 
        'NS_OPCO_BRAND' as model_nm,
        replace('NS_OPCO_BRAND', concat(split_part('NS_OPCO_BRAND', '_', 1), '_')) as target_model_nm,
        case 
            when replace('NS_OPCO_BRAND', concat(split_part('NS_OPCO_BRAND', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('NS_OPCO_BRAND', '_', 1) = 'AX' then 3
                when split_part('NS_OPCO_BRAND', '_', 1) = 'NS' then 3
                when split_part('NS_OPCO_BRAND', '_', 1) = 'JDE' then 1
                when split_part('NS_OPCO_BRAND', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('NS_OPCO_BRAND', '_', 1) = 'AX' then 'Incremental'
                when split_part('NS_OPCO_BRAND', '_', 1) = 'NS' then 'Incremental'
                when split_part('NS_OPCO_BRAND', '_', 1) = 'JDE' then 'Full Load'
                when split_part('NS_OPCO_BRAND', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.NS_OPCO_BRAND
        
         union all 
    
        select 
        'NS_OPCO_PURPOSE' as model_nm,
        replace('NS_OPCO_PURPOSE', concat(split_part('NS_OPCO_PURPOSE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('NS_OPCO_PURPOSE', concat(split_part('NS_OPCO_PURPOSE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('NS_OPCO_PURPOSE', '_', 1) = 'AX' then 3
                when split_part('NS_OPCO_PURPOSE', '_', 1) = 'NS' then 3
                when split_part('NS_OPCO_PURPOSE', '_', 1) = 'JDE' then 1
                when split_part('NS_OPCO_PURPOSE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('NS_OPCO_PURPOSE', '_', 1) = 'AX' then 'Incremental'
                when split_part('NS_OPCO_PURPOSE', '_', 1) = 'NS' then 'Incremental'
                when split_part('NS_OPCO_PURPOSE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('NS_OPCO_PURPOSE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.NS_OPCO_PURPOSE
        
         union all 
    
        select 
        'NS_OPCO_UOM' as model_nm,
        replace('NS_OPCO_UOM', concat(split_part('NS_OPCO_UOM', '_', 1), '_')) as target_model_nm,
        case 
            when replace('NS_OPCO_UOM', concat(split_part('NS_OPCO_UOM', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('NS_OPCO_UOM', '_', 1) = 'AX' then 3
                when split_part('NS_OPCO_UOM', '_', 1) = 'NS' then 3
                when split_part('NS_OPCO_UOM', '_', 1) = 'JDE' then 1
                when split_part('NS_OPCO_UOM', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('NS_OPCO_UOM', '_', 1) = 'AX' then 'Incremental'
                when split_part('NS_OPCO_UOM', '_', 1) = 'NS' then 'Incremental'
                when split_part('NS_OPCO_UOM', '_', 1) = 'JDE' then 'Full Load'
                when split_part('NS_OPCO_UOM', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.NS_OPCO_UOM
        
         union all 
    
        select 
        'JDE_OPCO_CURRENCY' as model_nm,
        replace('JDE_OPCO_CURRENCY', concat(split_part('JDE_OPCO_CURRENCY', '_', 1), '_')) as target_model_nm,
        case 
            when replace('JDE_OPCO_CURRENCY', concat(split_part('JDE_OPCO_CURRENCY', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('JDE_OPCO_CURRENCY', '_', 1) = 'AX' then 3
                when split_part('JDE_OPCO_CURRENCY', '_', 1) = 'NS' then 3
                when split_part('JDE_OPCO_CURRENCY', '_', 1) = 'JDE' then 1
                when split_part('JDE_OPCO_CURRENCY', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('JDE_OPCO_CURRENCY', '_', 1) = 'AX' then 'Incremental'
                when split_part('JDE_OPCO_CURRENCY', '_', 1) = 'NS' then 'Incremental'
                when split_part('JDE_OPCO_CURRENCY', '_', 1) = 'JDE' then 'Full Load'
                when split_part('JDE_OPCO_CURRENCY', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.JDE_OPCO_CURRENCY
        
         union all 
    
        select 
        'JDE_OPCO_FSCL_YR_GL_OPENING_BAL' as model_nm,
        replace('JDE_OPCO_FSCL_YR_GL_OPENING_BAL', concat(split_part('JDE_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1), '_')) as target_model_nm,
        case 
            when replace('JDE_OPCO_FSCL_YR_GL_OPENING_BAL', concat(split_part('JDE_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('JDE_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'AX' then 3
                when split_part('JDE_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'NS' then 3
                when split_part('JDE_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'JDE' then 1
                when split_part('JDE_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('JDE_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'AX' then 'Incremental'
                when split_part('JDE_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'NS' then 'Incremental'
                when split_part('JDE_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'JDE' then 'Full Load'
                when split_part('JDE_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.JDE_OPCO_FSCL_YR_GL_OPENING_BAL
        
         union all 
    
        select 
        'JDE_OPCO_CHART_OF_ACCTS' as model_nm,
        replace('JDE_OPCO_CHART_OF_ACCTS', concat(split_part('JDE_OPCO_CHART_OF_ACCTS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('JDE_OPCO_CHART_OF_ACCTS', concat(split_part('JDE_OPCO_CHART_OF_ACCTS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('JDE_OPCO_CHART_OF_ACCTS', '_', 1) = 'AX' then 3
                when split_part('JDE_OPCO_CHART_OF_ACCTS', '_', 1) = 'NS' then 3
                when split_part('JDE_OPCO_CHART_OF_ACCTS', '_', 1) = 'JDE' then 1
                when split_part('JDE_OPCO_CHART_OF_ACCTS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('JDE_OPCO_CHART_OF_ACCTS', '_', 1) = 'AX' then 'Incremental'
                when split_part('JDE_OPCO_CHART_OF_ACCTS', '_', 1) = 'NS' then 'Incremental'
                when split_part('JDE_OPCO_CHART_OF_ACCTS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('JDE_OPCO_CHART_OF_ACCTS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.JDE_OPCO_CHART_OF_ACCTS
        
         union all 
    
        select 
        'JDE_OPCO_GL_TRANS_TYPE' as model_nm,
        replace('JDE_OPCO_GL_TRANS_TYPE', concat(split_part('JDE_OPCO_GL_TRANS_TYPE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('JDE_OPCO_GL_TRANS_TYPE', concat(split_part('JDE_OPCO_GL_TRANS_TYPE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('JDE_OPCO_GL_TRANS_TYPE', '_', 1) = 'AX' then 3
                when split_part('JDE_OPCO_GL_TRANS_TYPE', '_', 1) = 'NS' then 3
                when split_part('JDE_OPCO_GL_TRANS_TYPE', '_', 1) = 'JDE' then 1
                when split_part('JDE_OPCO_GL_TRANS_TYPE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('JDE_OPCO_GL_TRANS_TYPE', '_', 1) = 'AX' then 'Incremental'
                when split_part('JDE_OPCO_GL_TRANS_TYPE', '_', 1) = 'NS' then 'Incremental'
                when split_part('JDE_OPCO_GL_TRANS_TYPE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('JDE_OPCO_GL_TRANS_TYPE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.JDE_OPCO_GL_TRANS_TYPE
        
         union all 
    
        select 
        'JDE_OPCO_GL_TRANS' as model_nm,
        replace('JDE_OPCO_GL_TRANS', concat(split_part('JDE_OPCO_GL_TRANS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('JDE_OPCO_GL_TRANS', concat(split_part('JDE_OPCO_GL_TRANS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('JDE_OPCO_GL_TRANS', '_', 1) = 'AX' then 3
                when split_part('JDE_OPCO_GL_TRANS', '_', 1) = 'NS' then 3
                when split_part('JDE_OPCO_GL_TRANS', '_', 1) = 'JDE' then 1
                when split_part('JDE_OPCO_GL_TRANS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('JDE_OPCO_GL_TRANS', '_', 1) = 'AX' then 'Incremental'
                when split_part('JDE_OPCO_GL_TRANS', '_', 1) = 'NS' then 'Incremental'
                when split_part('JDE_OPCO_GL_TRANS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('JDE_OPCO_GL_TRANS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.JDE_OPCO_GL_TRANS
        
         union all 
    
        select 
        'JDE_OPCO_COST_CENTER' as model_nm,
        replace('JDE_OPCO_COST_CENTER', concat(split_part('JDE_OPCO_COST_CENTER', '_', 1), '_')) as target_model_nm,
        case 
            when replace('JDE_OPCO_COST_CENTER', concat(split_part('JDE_OPCO_COST_CENTER', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('JDE_OPCO_COST_CENTER', '_', 1) = 'AX' then 3
                when split_part('JDE_OPCO_COST_CENTER', '_', 1) = 'NS' then 3
                when split_part('JDE_OPCO_COST_CENTER', '_', 1) = 'JDE' then 1
                when split_part('JDE_OPCO_COST_CENTER', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('JDE_OPCO_COST_CENTER', '_', 1) = 'AX' then 'Incremental'
                when split_part('JDE_OPCO_COST_CENTER', '_', 1) = 'NS' then 'Incremental'
                when split_part('JDE_OPCO_COST_CENTER', '_', 1) = 'JDE' then 'Full Load'
                when split_part('JDE_OPCO_COST_CENTER', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.JDE_OPCO_COST_CENTER
        
         union all 
    
        select 
        'JDE_OPCO' as model_nm,
        replace('JDE_OPCO', concat(split_part('JDE_OPCO', '_', 1), '_')) as target_model_nm,
        case 
            when replace('JDE_OPCO', concat(split_part('JDE_OPCO', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('JDE_OPCO', '_', 1) = 'AX' then 3
                when split_part('JDE_OPCO', '_', 1) = 'NS' then 3
                when split_part('JDE_OPCO', '_', 1) = 'JDE' then 1
                when split_part('JDE_OPCO', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('JDE_OPCO', '_', 1) = 'AX' then 'Incremental'
                when split_part('JDE_OPCO', '_', 1) = 'NS' then 'Incremental'
                when split_part('JDE_OPCO', '_', 1) = 'JDE' then 'Full Load'
                when split_part('JDE_OPCO', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.JDE_OPCO
        
         union all 
    
        select 
        'JDE_OPCO_UOM' as model_nm,
        replace('JDE_OPCO_UOM', concat(split_part('JDE_OPCO_UOM', '_', 1), '_')) as target_model_nm,
        case 
            when replace('JDE_OPCO_UOM', concat(split_part('JDE_OPCO_UOM', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('JDE_OPCO_UOM', '_', 1) = 'AX' then 3
                when split_part('JDE_OPCO_UOM', '_', 1) = 'NS' then 3
                when split_part('JDE_OPCO_UOM', '_', 1) = 'JDE' then 1
                when split_part('JDE_OPCO_UOM', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('JDE_OPCO_UOM', '_', 1) = 'AX' then 'Incremental'
                when split_part('JDE_OPCO_UOM', '_', 1) = 'NS' then 'Incremental'
                when split_part('JDE_OPCO_UOM', '_', 1) = 'JDE' then 'Full Load'
                when split_part('JDE_OPCO_UOM', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.JDE_OPCO_UOM
        
         union all 
    
        select 
        'PRIMEX_OPCO_CURRENCY' as model_nm,
        replace('PRIMEX_OPCO_CURRENCY', concat(split_part('PRIMEX_OPCO_CURRENCY', '_', 1), '_')) as target_model_nm,
        case 
            when replace('PRIMEX_OPCO_CURRENCY', concat(split_part('PRIMEX_OPCO_CURRENCY', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('PRIMEX_OPCO_CURRENCY', '_', 1) = 'AX' then 3
                when split_part('PRIMEX_OPCO_CURRENCY', '_', 1) = 'NS' then 3
                when split_part('PRIMEX_OPCO_CURRENCY', '_', 1) = 'JDE' then 1
                when split_part('PRIMEX_OPCO_CURRENCY', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('PRIMEX_OPCO_CURRENCY', '_', 1) = 'AX' then 'Incremental'
                when split_part('PRIMEX_OPCO_CURRENCY', '_', 1) = 'NS' then 'Incremental'
                when split_part('PRIMEX_OPCO_CURRENCY', '_', 1) = 'JDE' then 'Full Load'
                when split_part('PRIMEX_OPCO_CURRENCY', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.PRIMEX_OPCO_CURRENCY
        
         union all 
    
        select 
        'PRIMEX_OPCO_CHART_OF_ACCTS_TYPE' as model_nm,
        replace('PRIMEX_OPCO_CHART_OF_ACCTS_TYPE', concat(split_part('PRIMEX_OPCO_CHART_OF_ACCTS_TYPE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('PRIMEX_OPCO_CHART_OF_ACCTS_TYPE', concat(split_part('PRIMEX_OPCO_CHART_OF_ACCTS_TYPE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS_TYPE', '_', 1) = 'AX' then 3
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS_TYPE', '_', 1) = 'NS' then 3
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS_TYPE', '_', 1) = 'JDE' then 1
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS_TYPE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS_TYPE', '_', 1) = 'AX' then 'Incremental'
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS_TYPE', '_', 1) = 'NS' then 'Incremental'
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS_TYPE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS_TYPE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.PRIMEX_OPCO_CHART_OF_ACCTS_TYPE
        
         union all 
    
        select 
        'PRIMEX_OPCO_CHART_OF_ACCTS' as model_nm,
        replace('PRIMEX_OPCO_CHART_OF_ACCTS', concat(split_part('PRIMEX_OPCO_CHART_OF_ACCTS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('PRIMEX_OPCO_CHART_OF_ACCTS', concat(split_part('PRIMEX_OPCO_CHART_OF_ACCTS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS', '_', 1) = 'AX' then 3
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS', '_', 1) = 'NS' then 3
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS', '_', 1) = 'JDE' then 1
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS', '_', 1) = 'AX' then 'Incremental'
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS', '_', 1) = 'NS' then 'Incremental'
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('PRIMEX_OPCO_CHART_OF_ACCTS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.PRIMEX_OPCO_CHART_OF_ACCTS
        
         union all 
    
        select 
        'PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL' as model_nm,
        replace('PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL', concat(split_part('PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1), '_')) as target_model_nm,
        case 
            when replace('PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL', concat(split_part('PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'AX' then 3
                when split_part('PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'NS' then 3
                when split_part('PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'JDE' then 1
                when split_part('PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'AX' then 'Incremental'
                when split_part('PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'NS' then 'Incremental'
                when split_part('PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'JDE' then 'Full Load'
                when split_part('PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL
        
         union all 
    
        select 
        'PRIMEX_OPCO_GL_TRANS_TYPE' as model_nm,
        replace('PRIMEX_OPCO_GL_TRANS_TYPE', concat(split_part('PRIMEX_OPCO_GL_TRANS_TYPE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('PRIMEX_OPCO_GL_TRANS_TYPE', concat(split_part('PRIMEX_OPCO_GL_TRANS_TYPE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('PRIMEX_OPCO_GL_TRANS_TYPE', '_', 1) = 'AX' then 3
                when split_part('PRIMEX_OPCO_GL_TRANS_TYPE', '_', 1) = 'NS' then 3
                when split_part('PRIMEX_OPCO_GL_TRANS_TYPE', '_', 1) = 'JDE' then 1
                when split_part('PRIMEX_OPCO_GL_TRANS_TYPE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('PRIMEX_OPCO_GL_TRANS_TYPE', '_', 1) = 'AX' then 'Incremental'
                when split_part('PRIMEX_OPCO_GL_TRANS_TYPE', '_', 1) = 'NS' then 'Incremental'
                when split_part('PRIMEX_OPCO_GL_TRANS_TYPE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('PRIMEX_OPCO_GL_TRANS_TYPE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.PRIMEX_OPCO_GL_TRANS_TYPE
        
         union all 
    
        select 
        'PRIMEX_OPCO_GL_TRANS' as model_nm,
        replace('PRIMEX_OPCO_GL_TRANS', concat(split_part('PRIMEX_OPCO_GL_TRANS', '_', 1), '_')) as target_model_nm,
        case 
            when replace('PRIMEX_OPCO_GL_TRANS', concat(split_part('PRIMEX_OPCO_GL_TRANS', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('PRIMEX_OPCO_GL_TRANS', '_', 1) = 'AX' then 3
                when split_part('PRIMEX_OPCO_GL_TRANS', '_', 1) = 'NS' then 3
                when split_part('PRIMEX_OPCO_GL_TRANS', '_', 1) = 'JDE' then 1
                when split_part('PRIMEX_OPCO_GL_TRANS', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('PRIMEX_OPCO_GL_TRANS', '_', 1) = 'AX' then 'Incremental'
                when split_part('PRIMEX_OPCO_GL_TRANS', '_', 1) = 'NS' then 'Incremental'
                when split_part('PRIMEX_OPCO_GL_TRANS', '_', 1) = 'JDE' then 'Full Load'
                when split_part('PRIMEX_OPCO_GL_TRANS', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.PRIMEX_OPCO_GL_TRANS
        
         union all 
    
        select 
        'PRIMEX_OPCO_SUB_LEDGER_TYPE' as model_nm,
        replace('PRIMEX_OPCO_SUB_LEDGER_TYPE', concat(split_part('PRIMEX_OPCO_SUB_LEDGER_TYPE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('PRIMEX_OPCO_SUB_LEDGER_TYPE', concat(split_part('PRIMEX_OPCO_SUB_LEDGER_TYPE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('PRIMEX_OPCO_SUB_LEDGER_TYPE', '_', 1) = 'AX' then 3
                when split_part('PRIMEX_OPCO_SUB_LEDGER_TYPE', '_', 1) = 'NS' then 3
                when split_part('PRIMEX_OPCO_SUB_LEDGER_TYPE', '_', 1) = 'JDE' then 1
                when split_part('PRIMEX_OPCO_SUB_LEDGER_TYPE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('PRIMEX_OPCO_SUB_LEDGER_TYPE', '_', 1) = 'AX' then 'Incremental'
                when split_part('PRIMEX_OPCO_SUB_LEDGER_TYPE', '_', 1) = 'NS' then 'Incremental'
                when split_part('PRIMEX_OPCO_SUB_LEDGER_TYPE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('PRIMEX_OPCO_SUB_LEDGER_TYPE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.PRIMEX_OPCO_SUB_LEDGER_TYPE
        
         union all 
    
        select 
        'PRIMEX_OPCO_COST_CENTER' as model_nm,
        replace('PRIMEX_OPCO_COST_CENTER', concat(split_part('PRIMEX_OPCO_COST_CENTER', '_', 1), '_')) as target_model_nm,
        case 
            when replace('PRIMEX_OPCO_COST_CENTER', concat(split_part('PRIMEX_OPCO_COST_CENTER', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('PRIMEX_OPCO_COST_CENTER', '_', 1) = 'AX' then 3
                when split_part('PRIMEX_OPCO_COST_CENTER', '_', 1) = 'NS' then 3
                when split_part('PRIMEX_OPCO_COST_CENTER', '_', 1) = 'JDE' then 1
                when split_part('PRIMEX_OPCO_COST_CENTER', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('PRIMEX_OPCO_COST_CENTER', '_', 1) = 'AX' then 'Incremental'
                when split_part('PRIMEX_OPCO_COST_CENTER', '_', 1) = 'NS' then 'Incremental'
                when split_part('PRIMEX_OPCO_COST_CENTER', '_', 1) = 'JDE' then 'Full Load'
                when split_part('PRIMEX_OPCO_COST_CENTER', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.PRIMEX_OPCO_COST_CENTER
        
         union all 
    
        select 
        'PRIMEX_OPCO_DEPT' as model_nm,
        replace('PRIMEX_OPCO_DEPT', concat(split_part('PRIMEX_OPCO_DEPT', '_', 1), '_')) as target_model_nm,
        case 
            when replace('PRIMEX_OPCO_DEPT', concat(split_part('PRIMEX_OPCO_DEPT', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('PRIMEX_OPCO_DEPT', '_', 1) = 'AX' then 3
                when split_part('PRIMEX_OPCO_DEPT', '_', 1) = 'NS' then 3
                when split_part('PRIMEX_OPCO_DEPT', '_', 1) = 'JDE' then 1
                when split_part('PRIMEX_OPCO_DEPT', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('PRIMEX_OPCO_DEPT', '_', 1) = 'AX' then 'Incremental'
                when split_part('PRIMEX_OPCO_DEPT', '_', 1) = 'NS' then 'Incremental'
                when split_part('PRIMEX_OPCO_DEPT', '_', 1) = 'JDE' then 'Full Load'
                when split_part('PRIMEX_OPCO_DEPT', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.PRIMEX_OPCO_DEPT
        
         union all 
    
        select 
        'PRIMEX_OPCO_TYPE' as model_nm,
        replace('PRIMEX_OPCO_TYPE', concat(split_part('PRIMEX_OPCO_TYPE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('PRIMEX_OPCO_TYPE', concat(split_part('PRIMEX_OPCO_TYPE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('PRIMEX_OPCO_TYPE', '_', 1) = 'AX' then 3
                when split_part('PRIMEX_OPCO_TYPE', '_', 1) = 'NS' then 3
                when split_part('PRIMEX_OPCO_TYPE', '_', 1) = 'JDE' then 1
                when split_part('PRIMEX_OPCO_TYPE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('PRIMEX_OPCO_TYPE', '_', 1) = 'AX' then 'Incremental'
                when split_part('PRIMEX_OPCO_TYPE', '_', 1) = 'NS' then 'Incremental'
                when split_part('PRIMEX_OPCO_TYPE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('PRIMEX_OPCO_TYPE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.PRIMEX_OPCO_TYPE
        
         union all 
    
        select 
        'PRIMEX_OPCO_LOB' as model_nm,
        replace('PRIMEX_OPCO_LOB', concat(split_part('PRIMEX_OPCO_LOB', '_', 1), '_')) as target_model_nm,
        case 
            when replace('PRIMEX_OPCO_LOB', concat(split_part('PRIMEX_OPCO_LOB', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('PRIMEX_OPCO_LOB', '_', 1) = 'AX' then 3
                when split_part('PRIMEX_OPCO_LOB', '_', 1) = 'NS' then 3
                when split_part('PRIMEX_OPCO_LOB', '_', 1) = 'JDE' then 1
                when split_part('PRIMEX_OPCO_LOB', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('PRIMEX_OPCO_LOB', '_', 1) = 'AX' then 'Incremental'
                when split_part('PRIMEX_OPCO_LOB', '_', 1) = 'NS' then 'Incremental'
                when split_part('PRIMEX_OPCO_LOB', '_', 1) = 'JDE' then 'Full Load'
                when split_part('PRIMEX_OPCO_LOB', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.PRIMEX_OPCO_LOB
        
         union all 
    
        select 
        'PRIMEX_OPCO_PURPOSE' as model_nm,
        replace('PRIMEX_OPCO_PURPOSE', concat(split_part('PRIMEX_OPCO_PURPOSE', '_', 1), '_')) as target_model_nm,
        case 
            when replace('PRIMEX_OPCO_PURPOSE', concat(split_part('PRIMEX_OPCO_PURPOSE', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('PRIMEX_OPCO_PURPOSE', '_', 1) = 'AX' then 3
                when split_part('PRIMEX_OPCO_PURPOSE', '_', 1) = 'NS' then 3
                when split_part('PRIMEX_OPCO_PURPOSE', '_', 1) = 'JDE' then 1
                when split_part('PRIMEX_OPCO_PURPOSE', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('PRIMEX_OPCO_PURPOSE', '_', 1) = 'AX' then 'Incremental'
                when split_part('PRIMEX_OPCO_PURPOSE', '_', 1) = 'NS' then 'Incremental'
                when split_part('PRIMEX_OPCO_PURPOSE', '_', 1) = 'JDE' then 'Full Load'
                when split_part('PRIMEX_OPCO_PURPOSE', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from OI_DATA_DEV_V2.INTERMEDIATE_FND_BKP.PRIMEX_OPCO_PURPOSE
        
        
    
)

select * from _foundation_load_config
      );
    