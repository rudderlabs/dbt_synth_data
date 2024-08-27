{% macro synth_distribution_continuous_uniform(min=0, max=1) -%}
    {{ return(adapter.dispatch('synth_distribution_continuous_uniform')(min, max)) }}
{%- endmacro %}

{% macro default__synth_distribution_continuous_uniform(min, max) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro snowflake__synth_distribution_continuous_uniform(min, max) %}
    UNIFORM({{min}}::float, {{max}}::float, RANDOM( {{ dbt_synth_data.synth_get_randseed() }} ))
{% endmacro %}

    {% macro redshift__synth_distribution_continuous_uniform(min, max) %}
    (random() * ({{max}}-{{min}}) + {{min}})
{% endmacro %}

{% macro bigquery__synth_distribution_continuous_uniform(min, max) %}
    (rand() * ({{max}}-{{min}}) + {{min}})
{% endmacro %}