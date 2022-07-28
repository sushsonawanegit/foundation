with opco_opco_locn_xref as(
    select *,
    rank() over(partition by opco_sk, opco_locn_sk, opco_opco_locn_asscn_type_txt order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_opco_locn_xref_lineage') }}
),
final as(
    select 
    opco_sk, opco_locn_sk, opco_opco_locn_asscn_type_txt, crt_dtm, stg_load_dtm, delete_dtm,
    contact_type_txt, contact_nm, contact_ph_nbr, contact_ph_extn_nbr, contact_cell_nbr, contact_local_ph_nbr, contact_fax_nbr, contact_telex_nbr, contact_email_id, language_txt, start_dt, end_dt 
    from opco_opco_locn_xref
    where rnk = 1 and delete_dtm is null
)

select * from final