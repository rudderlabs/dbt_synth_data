{% macro synth_column_unique_key(name, null_frac=0) -%}
    {% set unique_field %}
        {{ adapter.dispatch('synth_column_unique_key')() }}
    {% endset %}

    {% set base_field %}
        CASE
            WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
            THEN NULL
            ELSE {{ unique_field }}
        END as {{name}}
    {% endset %}

    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}

{% macro default__synth_column_unique_key() -%}
    MD5( __row_number::varchar )
{%- endmacro %}

{% macro sqlite__synth_column_unique_key() %}
    {# SQLite doesn't support MD5() out-of-the-box, so just use the row number #}
    __row_number
{% endmacro %}

{% macro bigquery__synth_column_unique_key() -%}
    MD5(CAST(__row_number AS STRING))
{%- endmacro %}
