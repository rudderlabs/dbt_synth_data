{% macro synth_column_date(name, min='1990-01-01', max='', distribution='uniform', null_frac=0) -%}
    {% set base_field %}
        {{ adapter.dispatch('synth_column_date_base')(min, max, distribution, null_frac) }} AS {{name}}
    {% endset %}
    {{ synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}

{% macro default__synth_column_date_base(min, max, distribution, null_frac) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro sqlite__synth_column_date_base(min, max, distribution, null_frac) %}
    date('{{min}}',
        '+' ||
        ROUND({{dbt_synth_data.synth_sqlite_random()}} * ({% if max|length > 0 %}JULIANDAY('{{max}}'){% else %}JULIANDAY(DATE()){% endif %} - JULIANDAY('{{min}}'))) ||
        ' days')
{% endmacro %}

{% macro duckdb__synth_column_date_base(min, max, distribution, null_frac) %}
    date '{{min}}' + ROUND(RANDOM() * ({% if max|length > 0 %}date '{{max}}'{% else %}CURRENT_DATE{% endif %} - date '{{min}}'))::int
{% endmacro %}

                                                                                            {% macro postgres__synth_column_date_base(min, max, distribution) %}
    date '{{min}}' + ROUND(RANDOM() * ({% if max|length > 0 %}date '{{max}}'{% else %}CURRENT_DATE{% endif %} - date '{{min}}'))::int
{% endmacro %}

{% macro snowflake__synth_column_date_base(min, max, distribution, null_frac) %}
    {% set date_field %}
        dateadd(day, UNIFORM(0, datediff(day, '{{min}}'::date, '{{max}}'::date), RANDOM( {{ dbt_synth_data.synth_get_randseed() }} )), '{{min}}'::date)
    {% endset %}
    CASE
        WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
        THEN NULL
        ELSE {{ date_field }}
    END
{% endmacro%}

{% macro redshift__synth_column_date_base(min, max, distribution, null_frac) %}
    {% set date_field %}
        date '{{min}}' + ROUND(RANDOM() * ({% if max|length > 0 %}date '{{max}}'{% else %}CURRENT_DATE{% endif %} - date '{{min}}'))::int
    {% endset %}
    CASE
        WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
        THEN NULL
        ELSE {{ date_field }}
    END
{% endmacro %}

{% macro bigquery__synth_column_date_base(min, max, distribution, null_frac) %}
    {% set date_field %}
        date_add(date '{{ min }}', interval cast(round(rand()*date_diff(date '{{ max }}', date '{{ min }}', day)) as int64) day)
    {% endset %}
    CASE
        WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
        THEN NULL
        ELSE {{ date_field }}
    END
{% endmacro %}