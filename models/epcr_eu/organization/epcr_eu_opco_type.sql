{% set _load = load_type('EPCR_EU_OPCO_TYPE') %}

with epcr_eu_opco_type as (
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'EPCR-EU' as src_sys_nm,
    upper(trim(segmentcode)) as src_type_cd,
    max(segmentname) as src_type_desc,
    case 
        when activeflag =  'TRUE' then 1
        else 0
    end as actv_ind,
    {{ spcl_chr_rp('src_type_cd')}} as type_wo_spcl_chr_cd
    from {{source('EPCR_EU_DEV', 'COASEGVALUES')}}
    where coacode = 'MAIN' and segmentnbr = 1
    and company not in ('CUBAUS01','CUBAUS02') 
    group by src_sys_nm, src_type_cd, _fivetran_deleted, actv_ind

),
final as(
    select 
        {{dbt_utils.surrogate_key(['src_sys_nm', 'src_type_cd'])}} as opco_type_sk,
        {{dbt_utils.surrogate_key(['src_sys_nm', 'src_type_cd','src_type_desc','actv_ind','type_wo_spcl_chr_cd' ])}} as opco_type_ck, 
        *
    from epcr_eu_opco_type

),
delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'EPCR_EU_OPCO_TYPE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_TYPE') and _load != 3%}
        union
        select
        opco_type_sk, opco_type_ck, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, src_type_cd, src_type_desc, actv_ind, type_wo_spcl_chr_cd
         
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_type
        {% if _load == 1 %}
            where opco_type_ck not in (select distinct opco_type_ck from final)
        {% else %}
            where opco_type_ck not in (select distinct opco_type_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_type where src_sys_nm = 'EPCR-EU')
        {% endif %} 
        and src_sys_nm = 'EPCR-EU'
    {% endif %}
)

select * from delete_captured


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_type_ck not in (select distinct opco_type_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_type where src_sys_nm = 'EPCR-EU')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_type_ck in (select distinct opco_type_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_type where src_sys_nm = 'EPCR-EU') and delete_dtm is not null)
    {% endif %}
{% endif %}
