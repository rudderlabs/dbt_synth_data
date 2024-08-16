{% macro synth_column_timestamp(name, min='2020-01-01 00:00:00.000', max='2030-12-31 23:59:59.999', distribution='uniform', null_frac=0) -%}
    {% set base_field %}
        {{ adapter.dispatch('synth_column_timestamp_base')(min, max, distribution, null_frac) }} AS {{name}}
    {% endset %}
    {{ synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}

{% macro default__synth_column_timestamp_base(min, max, distribution, null_frac) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_column_timestamp_base(min, max, distribution, null_frac) %}
    date '{{min}}' + ROUND(RANDOM() * ({% if max|length > 0 %}date '{{max}}'{% else %}CURRENT_DATE{% endif %} - date '{{min}}'))::int
{% endmacro %}

{% macro snowflake__synth_column_timestamp_base(min, max, distribution, null_frac) %}
    {% set date_field %}
        dateadd(
            milliseconds,
            UNIFORM(
                0,
                datediff(milliseconds, '{{min}}'::timestamp, '{{max}}'::timestamp),
                RANDOM( {{ dbt_synth_data.synth_get_randseed() }} )),
            '{{min}}'::timestamp
        )
    {% endset %}
    CASE
        WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
        THEN NULL
        ELSE {{ date_field }}
    END
{% endmacro%}