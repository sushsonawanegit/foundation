

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_cust  as
      (with opco_cust as(
    select *,
    rank() over(partition by opco_cust_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_cust_lineage
),
final as(
    select 
    opco_cust_sk, opco_cust_ck, crt_dtm, stg_load_dtm, delete_dtm,
    src_sys_nm, cust_id, opco_id, dba_nm, govt_issued_id, duns_nbr, lob_nm, vat_number, language_txt, country_cd, website_url_txt,
    tax_exempt_nbr, tax_grp_txt, cust_item_grp_cd, end_market_nm, collector_cd, party_id, flag_id

    from opco_cust
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    