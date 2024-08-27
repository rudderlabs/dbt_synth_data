{% macro synth_column_integer(name, min=None, max=None, null_frac=0) -%}
    {% if min is none or max is none %}
        {{ exceptions.raise_compiler_error("integer column `" + name + "` must specify `min` and `max`") }}
    {% endif %}
    
    {% set integer_field %}
        cast({{ dbt_synth_data.synth_distribution_discretize_floor(distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=min, max=max+1)) }} as int64)
    {% endset %}

    {% set integer_type %}
        {% if target.type == "bigquery" %}
            cast({{ integer_field }} as int64)
        {% else %}
            {{ integer_field }}
        {% endif %}
    {% endset %}

    {% set base_field %}
        CASE
            WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
            THEN NULL
            ELSE {{ integer_type }}
        END as {{name}}
    {% endset %}

    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}