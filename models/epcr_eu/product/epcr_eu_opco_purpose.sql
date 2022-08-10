{% set _load = load_type('EPCR_EU_OPCO_PURPOSE') %}

with epcr_eu_opco_purpose as (
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'EPCR-EU' as src_sys_nm,
    upper(trim(segmentcode)) as src_purpose_cd,
    max(segmentname) as src_purpose_desc,
    case 
        when activeflag =  'TRUE' then 1
        else 0
    end as actv_ind,
    {{ spcl_chr_rp('src_purpose_cd')}} as purpose_wo_spcl_chr_cd
    from {{source('EPCR_EU_DEV', 'COASEGVALUES')}}
    where coacode = 'MAIN' and segmentnbr = 4
    and company not in ('CUBAUS01','CUBAUS02') 
    group by src_sys_nm, src_purpose_cd, _fivetran_deleted, actv_ind

),
final as(
    select 
        {{dbt_utils.surrogate_key(['src_sys_nm', 'src_purpose_cd'])}} as opco_purpose_sk,
        {{dbt_utils.surrogate_key(['src_sys_nm', 'src_purpose_cd','src_purpose_desc','actv_ind','purpose_wo_spcl_chr_cd' ])}} as opco_purpose_ck, 
        *
    from epcr_eu_opco_purpose

),
delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'EPCR_EU_OPCO_PURPOSE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PURPOSE') and _load != 3%}
        union
        select
        opco_purpose_sk, opco_purpose_ck, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, src_purpose_cd, src_purpose_desc, actv_ind, purpose_wo_spcl_chr_cd
         
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_purpose
        {% if _load == 1 %}
            where opco_purpose_ck not in (select distinct opco_purpose_ck from final)
        {% else %}
            where opco_purpose_ck not in (select distinct opco_purpose_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_purpose where src_sys_nm = 'EPCR-EU')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_purpose_ck not in (select distinct opco_purpose_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_purpose where src_sys_nm = 'EPCR-EU')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_purpose_ck in (select distinct opco_purpose_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_purpose where src_sys_nm = 'EPCR-EU') and delete_dtm is not null)
    {% endif %}
{% endif %}
