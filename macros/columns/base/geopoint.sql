{% macro synth_column_geopoint(name) -%}
    {% set base_field %}
        {{ adapter.dispatch('synth_column_geopoint_base')() }} AS {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}

{% macro default__synth_column_geopoint_base() -%}
   {{ exceptions.raise_compiler_error("Error for column column `" ~ name ~ "`: geopoint is unavailable for SQLite.") }}
{%- endmacro %}

{% macro postgres__synth_column_geopoint_base() %}
    ST_MAKEPOINT(
        RANDOM()*360.0 - 180.0,
        RANDOM()*180.0 - 90.0
    )
{% endmacro %}

{% macro redshift__synth_column_geopoint_base() %}
    ST_MAKEPOINT(
        RANDOM()*360.0 - 180.0,
        RANDOM()*180.0 - 90.0
    )
{% endmacro %}

{% macro snowflake__synth_column_geopoint_base() %}
    ST_MAKEPOINT(
        UNIFORM(-180.0, 180.0, RANDOM( {{ dbt_synth_data.synth_get_randseed() }} )),
        UNIFORM(-90.0, 90.0, RANDOM( {{ dbt_synth_data.synth_get_randseed() }} ))
    )
{% endmacro%}