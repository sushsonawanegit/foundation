{% set _load = load_type('EPCR_EU_OPCO_COST_CENTER') %}

with epcr_eu_opco_cost_center as (
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'EPCR-EU' as src_sys_nm,
    upper(trim(segmentcode)) as src_cost_center_cd,
    max(segmentname)  as src_cost_center_desc,
    case
    when activeflag = 'TRUE' then 1 
    else 0
    end as actv_ind,
    null as src_cost_center_type_txt,
    {{ spcl_chr_rp('src_cost_center_cd')}} as cost_center_wo_spcl_chr_cd
    from {{source('EPCR_EU_DEV', 'COASEGVALUES')}}
    where coacode = 'MAIN' and segmentnbr = 2
    and company not in ('CUBAUS01','CUBAUS02') 
    group by src_sys_nm, src_cost_center_cd , _fivetran_deleted, actv_ind

),
final as(
    select 
        {{dbt_utils.surrogate_key(['src_sys_nm', 'src_cost_center_cd'])}} as opco_cost_center_sk,
        {{dbt_utils.surrogate_key(['src_sys_nm', 'src_cost_center_cd','src_cost_center_desc','actv_ind','src_cost_center_type_txt','cost_center_wo_spcl_chr_cd' ])}} as opco_cost_center_ck, 
        *
    from epcr_eu_opco_cost_center

),
delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'EPCR_EU_OPCO_COST_CENTER') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_COST_CENTER') and _load != 3%}
        union
        select
        opco_cost_center_sk, opco_cost_center_ck, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, src_cost_center_cd, src_cost_center_desc, actv_ind, src_cost_center_type_txt, cost_center_wo_spcl_chr_cd
         
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cost_center
        {% if _load == 1 %}
            where opco_cost_center_ck not in (select distinct opco_cost_center_ck from final)
        {% else %}
            where opco_cost_center_ck not in (select distinct opco_cost_center_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cost_center where src_sys_nm = 'EPCR-EU')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cost_center_ck not in (select distinct opco_cost_center_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cost_center where src_sys_nm = 'EPCR-EU')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_cost_center_ck in (select distinct opco_cost_center_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_cost_center where src_sys_nm = 'EPCR-EU') and delete_dtm is not null)
    {% endif %}
{% endif %}

