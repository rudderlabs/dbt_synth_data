{% macro synth_column_email_address(
    name,
    distribution="weighted"
) -%}

    {% set email_address_expression %}
        lower( {{name}}__firstname || '.' || {{name}}__lastname || '@' || {{name}}__email_domain )
    {% endset %}

    {{ dbt_synth_data.synth_column_firstname(name=name+'__firstname', distribution=distribution, filter=filter) }}
    {{ dbt_synth_data.synth_remove('final_fields', name+"__firstname") }}

    {{ dbt_synth_data.synth_column_lastname(name=name+'__lastname', distribution=distribution, filter=filter) }}
    {{ dbt_synth_data.synth_remove('final_fields', name+"__lastname") }}

    {{ dbt_synth_data.synth_column_email_domain(name=name+'__email_domain', distribution=distribution, filter=filter) }}
    {{ dbt_synth_data.synth_remove('final_fields', name+"__email_domain") }}

    {% set final_field %}
        {{email_address_expression}} as {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
    {{ return("") }}
{%- endmacro %}

