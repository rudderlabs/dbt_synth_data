{% macro synth_column_string(name, min_length=1, max_length=32, null_frac=0) -%}
    {{ return(adapter.dispatch('synth_column_string_base')(name, min_length, max_length, null_frac)) }}
{%- endmacro %}

{% macro default__synth_column_string_base(name, min_length, max_length, null_frac) -%}
    {% set allowed_chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz' %}
    {% set string_field %}
        (
            substr(
                {% for i in range(0,max_length) %}
                    substr(
                        '{{allowed_chars}}',
                        cast( {{ dbt_synth_data.synth_distribution_discretize_floor(
                            distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=0, max=allowed_chars|length)
                        ) }} as int),
                        1
                    )
                    {% if not loop.last %} || {% endif %}
                {% endfor %}
            , 0, cast( {{ dbt_synth_data.synth_distribution_discretize_floor(
                distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=min_length, max=max_length+1)
            ) }} as int) )
        )
    {% endset %}

    {% set base_field %}
        CASE
            WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
            THEN NULL
            ELSE {{ string_field }}
        END as {{ name }}
    {% endset %}

    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}

{% macro snowflake__synth_column_string_base(name, min_length, max_length, null_frac) %}
    {% set string_field %}
        (
        randstr(
            uniform({{min_length}}, {{max_length}}, RANDOM({{ dbt_synth_data.synth_get_randseed() }}) ),
            uniform(1, 1000000000, RANDOM({{ dbt_synth_data.synth_get_randseed() }}) )
        )::varchar({{max_length}})
        )
    {% endset %}

    {% set base_field %}
        CASE
            WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
            THEN NULL
            ELSE {{ string_field }}
        END as {{ name }}
    {% endset %}

    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}
    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}

{% endmacro%}

{% macro redshift__synth_column_string_base(name, min_length, max_length, null_frac) -%}
    {% set allowed_chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz' %}
    {% set string_field %}
        (
            substring(
                {% for i in range(0,max_length) %}
                    substring(
                        '{{allowed_chars}}',
                        cast( {{ dbt_synth_data.synth_distribution_discretize_floor(
                            distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=0, max=allowed_chars|length)
                        ) }} as int),
                        1
                    )
                    {% if not loop.last %} || {% endif %}
                {% endfor %}
            , 0, cast( {{ dbt_synth_data.synth_distribution_discretize_floor(
                distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=min_length, max=max_length+1)
            ) }} as int) )
        )
    {% endset %}

    {% set base_field %}
        CASE
            WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
            THEN NULL
            ELSE {{ string_field }}
        END as {{ name }}
    {% endset %}

    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}