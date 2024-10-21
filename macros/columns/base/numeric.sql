{% macro synth_column_numeric(name, min, max, precision=5, null_frac=0) -%}
    {% set numeric_field %}
        {{ dbt_synth_data.synth_distribution_discretize_round(
            distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=min, max=max),
            precision=precision
        ) }}
    {% endset %}

    {% set base_field %}
        CASE
            WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
            THEN NULL
            ELSE {{ numeric_field }}
        END as {{name}}
    {% endset %}

    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}
