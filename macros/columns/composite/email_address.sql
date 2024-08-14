{% macro synth_column_email_address(
    name,
    distribution="weighted",
    null_frac=0
) -%}

    {% set email_address_expression %}
        lower( {{name}}__firstname || '.' || {{name}}__lastname || '@' || {{name}}__email_domain )
    {% endset %}

    {% set email_address_field %}
        CASE
            WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
            THEN NULL
            ELSE {{ email_address_expression }}
        END
    {% endset %}

    {{ dbt_synth_data.synth_column_firstname(name=name+'__firstname', distribution=distribution, filter=filter) }}
    {{ dbt_synth_data.synth_remove('final_fields', name+"__firstname") }}

    {{ dbt_synth_data.synth_column_lastname(name=name+'__lastname', distribution=distribution, filter=filter) }}
    {{ dbt_synth_data.synth_remove('final_fields', name+"__lastname") }}

    {{ dbt_synth_data.synth_column_email_domain(name=name+'__email_domain', distribution=distribution, filter=filter) }}
    {{ dbt_synth_data.synth_remove('final_fields', name+"__email_domain") }}

    {% set final_field %}
        {{email_address_field}} as {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
    {{ return("") }}
{%- endmacro %}

