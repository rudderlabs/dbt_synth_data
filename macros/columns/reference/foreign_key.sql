{% macro synth_column_foreign_key(name, model_name, column, distribution="uniform", weight_col="", filter="", null_frac=0.0) -%}
    {{ dbt_synth_data.synth_column_select(name, model_name, value_cols=[column], distribution=distribution, weight_col=weightcol, filter=filter, null_frac=null_frac) or "" }}
{%- endmacro %}