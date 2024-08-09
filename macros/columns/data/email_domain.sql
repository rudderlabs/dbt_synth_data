{% macro synth_column_email_domain(name, distribution="weighted", weight_col="weight", filter="") -%}
    {{ dbt_synth_data.synth_column_select(
        name=name,
        model_name="synth_email_domains",
        value_cols="name",
        distribution=distribution,
        weight_col=weight_col,
        filter=filter,
    ) }}
    {{ return("") }}
{%- endmacro %}