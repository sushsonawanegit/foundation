with _foundation_load_config as(
    
    {% set models = [
                    'AX_OPCO_CASH_DSCNT_TERMS','AX_OPCO_COMMSN_GRP','AX_OPCO_CURRENCY','AX_OPCO_CUST_CODE','AX_OPCO_CUST_GRP','AX_OPCO_CUST_INVOICE_LINE','AX_OPCO_CUST_INVOICE','AX_OPCO_CUST_OPEN_TRANS','AX_OPCO_CUST_PACKING_SLIP_LINE','AX_OPCO_CUST_PACKING_SLIP','AX_OPCO_CUST_SETTLED_TRANS','AX_OPCO_CUST_TRANS','AX_OPCO_CUST_TYPE','AX_OPCO_CUST','AX_OPCO_OPCO_CUST_XREF','AX_OPCO_PICKING_LIST','AX_OPCO_PYMNT_MODE','AX_OPCO_PYMNT_TERMS',
                    'AX_CHART_OF_ACCTS','AX_OPCO_CHART_OF_ACCTS','AX_OPCO_FSCL_YR_GL_OPENING_BAL','AX_OPCO_GL_TRANS', 
                    'AX_OPCO_CUST_OPCO_LOCN_XREF','AX_OPCO_LOCN','AX_OPCO_OPCO_LOCN_XREF','AX_OPCO_VENDOR_OPCO_LOCN_XREF', 
                    'AX_FSCL_CLNDR','AX_OPCO_ASSCTN','AX_OPCO_COST_CENTER','AX_OPCO_DEPT','AX_OPCO_SITE','AX_OPCO_TYPE','AX_OPCO','AX_REGION','AX_SUB_REGION','AX_WAREHOUSE',                    
                    'AX_OPCO_INVTRY_TRANS','AX_OPCO_ITEM_CLASS','AX_OPCO_INVTRY_TRANS','AX_OPCO_ITEM_CVRG_GRP','AX_OPCO_ITEM_FREIGHT','AX_OPCO_ITEM_GRP','AX_OPCO_ITEM_INVTRY','AX_OPCO_ITEM_MODEL_GRP','AX_OPCO_ITEM_OPCO_ASSCTN_XREF','AX_OPCO_ITEM_PSI_DTL','AX_OPCO_ITEM_SIZE','AX_OPCO_ITEM_SLS_CLASS','AX_OPCO_ITEM_SUBTYPE','AX_OPCO_ITEM_TYPE','AX_OPCO_ITEM','AX_OPCO_LOB','AX_OPCO_PURPOSE','AX_OPCO_UOM','AX_OPCO_VENDOR',
                    'AX_OPCO_PROJECT_CATGRY','AX_OPCO_PROJECT_COST_FRCST','AX_OPCO_PROJECT_COST_TRANS','AX_OPCO_PROJECT_EMPL_FRCST','AX_OPCO_PROJECT_EMPL_TRANS','AX_OPCO_PROJECT_INVOICE_PROJECT_XREF','AX_OPCO_PROJECT_ITEM_TRANS','AX_OPCO_PROJECT_REVENUE_TRANS','AX_OPCO_PROJECT_TRANS_POSTING','AX_OPCO_PROJECT',
                    'AX_OPCO_PO_LINE','AX_OPCO_PO',
                    'AX_OPCO_DLVRY_MODE','AX_OPCO_DLVRY_TERMS','AX_OPCO_SLS_ORDR_LINE','AX_OPCO_SLS_ORDR','AX_SLS_ORDR_OPCO_LOCN_XREF',
                    'AX_OPCO_VENDOR_INVOICE_LINE','AX_OPCO_VENDOR_INVOICE','AX_OPCO_VENDOR_PACKING_SLIP_LINE','AX_OPCO_VENDOR_PACKING_SLIP','AX_OPCO_VENDOR_TRANS',

                    'NS_OPCO_CURRENCY','NS_OPCO_PYMNT_MODE','NS_OPCO_PYMNT_TERMS',
                    'NS_OPCO_CHART_OF_ACCTS','NS_OPCO_FSCL_YR_GL_OPENING_BAL','NS_OPCO_GL_TRANS',
                    'NS_OPCO_COST_CENTER','NS_OPCO_DEPT','NS_OPCO_TYPE','NS_OPCO','NS_REGION','NS_SUB_REGION',
                    'NS_OPCO_BRAND','NS_OPCO_PURPOSE','NS_OPCO_UOM',

                    'JDE_OPCO_CURRENCY', 
                    'JDE_OPCO_FSCL_YR_GL_OPENING_BAL','JDE_OPCO_CHART_OF_ACCTS', 'JDE_OPCO_GL_TRANS_TYPE', 'JDE_OPCO_GL_TRANS',
                    'JDE_OPCO_COST_CENTER', 'JDE_OPCO', 
                    'JDE_OPCO_UOM', 

                    'PRIMEX_OPCO_CURRENCY',
                    'PRIMEX_OPCO_CHART_OF_ACCTS_TYPE', 'PRIMEX_OPCO_CHART_OF_ACCTS', 'PRIMEX_OPCO_FSCL_YR_GL_OPENING_BAL', 'PRIMEX_OPCO_GL_TRANS_TYPE', 'PRIMEX_OPCO_GL_TRANS', 'PRIMEX_OPCO_SUB_LEDGER_TYPE',
                    'PRIMEX_OPCO_COST_CENTER', 'PRIMEX_OPCO_DEPT', 'PRIMEX_OPCO_TYPE',
                    'PRIMEX_OPCO_LOB', 'PRIMEX_OPCO_PURPOSE'] %}

    {% for _model in models %}
        select 
        '{{_model}}' as model_nm,
        replace('{{_model}}', concat(split_part('{{_model}}', '_', 1), '_')) as target_model_nm,
        case 
            when replace('{{_model}}', concat(split_part('{{_model}}', '_', 1), '_')) = 'OPCO_GL_TRANS' then 'TRANS_DT'
            else null 
        end as incrmnt_load_on,
        {% if tb_check( var('fnd_tbl_db'), var('intermediate_tbl_sch'), _model) %}
            max(stg_load_dtm) as max_stg_load_dtm,
            case 
                when split_part('{{_model}}', '_', 1) = 'AX' then 3
                when split_part('{{_model}}', '_', 1) = 'NS' then 3
                when split_part('{{_model}}', '_', 1) = 'JDE' then 1
                when split_part('{{_model}}', '_', 1) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('{{_model}}', '_', 1) = 'AX' then 'Incremental'
                when split_part('{{_model}}', '_', 1) = 'NS' then 'Incremental'
                when split_part('{{_model}}', '_', 1) = 'JDE' then 'Full Load'
                when split_part('{{_model}}', '_', 1) = 'PRIMEX' then 'Full Load'
            end as load_desc
            from {{ var('fnd_tbl_db')}}.{{ var('intermediate_tbl_sch')}}.{{_model}}
        {% else %}
            null::timestamp_tz as max_stg_load_dtm,
            case 
                when split_part('{{_model}}', '_', 0) = 'AX' then 3
                when split_part('{{_model}}', '_', 0) = 'NS' then 3
                when split_part('{{_model}}', '_', 0) = 'JDE' then 1
                when split_part('{{_model}}', '_', 0) = 'PRIMEX' then 1
            end as load_type,
            case 
                when split_part('{{_model}}', '_', 0) = 'AX' then 'Incremental'
                when split_part('{{_model}}', '_', 0) = 'NS' then 'Incremental'
                when split_part('{{_model}}', '_', 0) = 'JDE' then 'Full Load'
                when split_part('{{_model}}', '_', 0) = 'PRIMEX' then 'Full Load'
            end as load_desc
        {% endif %}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select * from _foundation_load_config