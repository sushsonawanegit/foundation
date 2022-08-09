{% set _load = load_type('SYSPRO_PRIMEX_OPCO_LOB') %}

with syspro_primex_opco_lob as(
    {% for sch in var('primex_schemas') %}
        {% if tb_check(var('primex_db'), sch, 'CUSGENMASTER_') %}
            select
            substr('{{sch}}', 22, 1) as src_comp,
            current_timestamp as crt_dtm,
            mule_load_ts as stg_load_dtm,
            null::timestamp_tz as delete_dtm,
            'SYSPRO-PRMX' as src_sys_nm,
            upper(trim(fsgroup4buname)) as src_lob_cd,
            trim(fsgroup4buname) as src_lob_nm,
            1::number(1,0) as actv_ind,
            {{ spcl_chr_rp('src_lob_nm')}} as lob_wo_spcl_chr_cd
            from {{ var('primex_db')}}.{{ sch}}.CUSGENMASTER_
            {% if not loop.last %} union all {% endif %}
        {% endif %}
    {% endfor %}
),
de_duplication as(
    select *
           , rank() over(partition by src_lob_cd order by stg_load_dtm, src_comp) as rnk
    from syspro_primex_opco_lob
),
final as(
    select distinct
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_lob_cd']) }} as opco_lob_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_lob_cd', 'src_lob_nm','actv_ind', 'lob_wo_spcl_chr_cd']) }} as opco_lob_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_lob_cd, src_lob_nm, actv_ind, lob_wo_spcl_chr_cd
    from de_duplication
    where rnk = 1 
),
delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'SYSPRO_PRIMEX_OPCO_LOB') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_LOB') and _load != 3%}
        union
        select
        OPCO_LOB_SK, OPCO_LOB_CK, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, src_lob_cd, src_lob_nm, actv_ind, lob_wo_spcl_chr_cd
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOB
        {% if _load == 1 %}
            where OPCO_LOB_ck not in (select distinct OPCO_LOB_ck from final)
        {% else %}
            where OPCO_LOB_ck not in (select distinct OPCO_LOB_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOB where src_sys_nm = 'SYSPRO-PRMX')
        {% endif %} 
        and src_sys_nm = 'SYSPRO-PRMX'
    {% endif %}
)

select * from delete_captured


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_LOB_ck not in (select distinct OPCO_LOB_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOB where src_sys_nm = 'SYSPRO-PRMX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_LOB_ck in (select distinct OPCO_LOB_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOB where src_sys_nm = 'SYSPRO-PRMX') and delete_dtm is not null)
    {% endif %}
{% endif %}