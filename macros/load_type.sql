{% macro load_type(tb_nm) %}

    {% set myquery %}
        SELECT LOAD_TYPE from {{ var('fnd_tbl_db')}}.{{ var('intermediate_tbl_sch')}}._FOUNDATION_LOAD_CONFIG WHERE MODEL_NM = '{{tb_nm}}'
    {% endset %}
    {% set results=run_query(myquery) %}
    {% if execute %}
        {% set rset=results.columns[0].values()[0] %}
    {% endif %}

    {{ return(rset)}}

{% endmacro %}