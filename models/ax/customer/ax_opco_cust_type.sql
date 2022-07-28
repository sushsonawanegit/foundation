{% set _load = load_type('AX_OPCO_CUST_TYPE') %}

with ax_opco_cust_type as(
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(category_opi) as src_cust_type_cd,
    max(description_opi) as src_cust_type_desc
    from {{source('AX_DEV', 'CUSTPRECASTCATEGORY_OPI')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_cust_type_cd, _fivetran_deleted
),
final as(
    select  {{dbt_utils.surrogate_key(['src_sys_nm', 'src_cust_type_cd'])}} as opco_cust_type_sk,
            {{dbt_utils.surrogate_key(['src_sys_nm', 'src_cust_type_cd', 'src_cust_type_desc'])}} as opco_cust_type_ck,  
            *
    from ax_opco_cust_type 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_CUST_TYPE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_CUST_TYPE') and _load != 3%}
    union
    select
    opco_cust_type_sk, opco_cust_type_ck, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, src_cust_type_cd, src_cust_type_desc
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CUST_TYPE
    {% if _load == 1 %}
        where opco_cust_type_ck not in (select distinct opco_cust_type_ck from final)
    {% else %}
        where opco_cust_type_ck not in (select distinct opco_cust_type_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CUST_TYPE where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_type_ck not in (select distinct opco_cust_type_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CUST_TYPE where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cust_type_ck in (select distinct opco_cust_type_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_CUST_TYPE where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}