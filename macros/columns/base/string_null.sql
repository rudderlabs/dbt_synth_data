{% macro synth_column_string_null(
    name,
    min_length=1,
    max_length=32,
    null_frac=0.2
) -%}
    {% set base_field %}
        CASE
            WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
            THEN NULL
            ELSE {{ name }}__string
        END as {{name}}
    {% endset %}

    {{ dbt_synth_data.synth_column_string(name=name+'__string', min_length=1, max_length=32) }}

    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}
    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}