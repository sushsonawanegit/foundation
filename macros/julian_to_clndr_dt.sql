{% macro clndr_yr(dt) %}
    concat((substring({{ dt}}, 1, 1)::int + 19)::varchar, substring({{ dt}}, 2, 2))
{% endmacro %}

{% macro clndr_day(dt) %}
    substring({{ dt}}, 4, 3)
{% endmacro %}

{% macro hr(ts) %}
    substring({{ ts}}, 1, 2)
{% endmacro %}

{% macro mn(ts) %}
    substring({{ ts}}, 3, 2)
{% endmacro %}

{% macro sec(ts) %}
    substring({{ ts}}, 5, 2)
{% endmacro %}

{% macro julian_to_clndr_dt(dt, ts = none) %}
    {% if ts is none %}
        date_from_parts({{ clndr_yr(dt|string)}}, 1, {{ clndr_day(dt|string)}})
    {% else %}
        timestamp_tz_from_parts({{ clndr_yr(dt|string)}}, 1, {{ clndr_day(dt|string)}}, {{ hr(ts|string)}}, {{ mn(ts|string)}}, {{ sec(ts|string)}})
    {% endif %}
{% endmacro %}