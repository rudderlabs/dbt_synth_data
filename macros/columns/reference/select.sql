{% macro synth_column_select(name, model_name, value_cols=[], distribution="uniform", weight_col="", filter="", do_ref=True, null_frac=0) -%}
    {# Allow for `value_cols` to be a single (string) column name: #}
    {% if value_cols is string %}{% set value_cols = [value_cols] %}{% endif %}
    
    {% if distribution=='uniform' %}
        {{ dbt_synth_data.synth_column_select_uniform(name, model_name, value_cols, filter, do_ref, null_frac) }}
    
    {% elif distribution=='weighted' %}
        {{ dbt_synth_data.synth_column_select_weighted(name, model_name, value_cols, weight_col, filter, do_ref, null_frac) }}
    
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid `distribution` " ~ distribution ~ " for select column `" ~ name ~ "`: should be `uniform` (default) or `weighted`.") }}
    {% endif %}
{%- endmacro %}

{% macro synth_column_select_uniform(name, model_name, value_cols, filter, do_ref, null_frac) %}
    {{ return(adapter.dispatch('synth_column_select_uniform')(name, model_name, value_cols, filter, do_ref, null_frac)) }}
{% endmacro %}

{% macro default__synth_column_select_uniform(name, model_name, value_cols, filter, do_ref, null_frac) %}
    {% set table_name = dbt_synth_data.synth_retrieve('synth_conf')['table_name'] or "synth_table" %}
    {% set cte %}
        {# {{table_name}}__{{name}}__cte as ( #}
            select
                {% for value_col in value_cols %}
                {{value_col}},
                {% endfor %}
                1.0*( (row_number() over (order by {{value_cols[0]}} asc)) - 1 ) / count(*) over () as from_val,
                1.0*( (row_number() over (order by {{value_cols[0]}} asc))     ) / count(*) over () as to_val
            from {% if do_ref %}{{ref(model_name)}}{% else %}{{model_name}}{% endif %}
            {% if filter|trim|length %}
            where {{filter}}
            {% endif %}
            order by from_val asc, to_val asc
        {# ) #}
    {% endset %}
    {{ dbt_synth_data.synth_store("ctes", name+"__cte", cte) }}

    {% set base_field %}
        {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0, max=1) }} as {{name}}__rand
    {% endset %}
    {{ dbt_synth_data.synth_store("base_fields", name+"__rand", base_field) }}

    {% set join_fields %}
        {% if value_cols|length==1 %}
            {{table_name}}__{{name}}__cte.{{value_cols[0]}} as {{name}}
        {% else %}
            {% for value_col in value_cols %}
            {{table_name}}__{{name}}__cte.{{value_col}} as {{name}}__{{value_col}}
            {% if not loop.last %},{% endif %}
            {% endfor%}
        {% endif %}
    {% endset %}
    {% set join_clause %}
        left join {{table_name}}__{{name}}__cte on ___PREVIOUS_CTE___.{{name}}__rand between {{table_name}}__{{name}}__cte.from_val and {{table_name}}__{{name}}__cte.to_val
    {% endset %}
    {{ dbt_synth_data.synth_store("joins", name+"__cte", {"fields": join_fields, "clause": join_clause} ) }}

    {% set uniform_field %}
        {% if value_cols|length==1 %}
            {{name}}
        {% else %}
            {% for value_col in value_cols %}
            {{name}}__{{value_col}}
            {% if not loop.last %},{% endif %}
            {% endfor %}
        {% endif %}
    {% endset %}

    {% set final_field %}
        (
        CASE
            WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
            THEN NULL
            ELSE {{ uniform_field }}
        END
        ) as {{ name }}
    {% endset %}

    {{ dbt_synth_data.synth_store("final_fields", name, final_field) }}
{% endmacro %}

{% macro bigquery__synth_column_select_uniform(name, model_name, value_cols, filter, do_ref, null_frac) %}
    {% set table_name = target.schema ~ "." ~ (model.name or "synth_table") %}
    {% set table_alias = model.name or "synth_table" %}

    {% set cte %}
        {# {{table_name}}__{{name}}__cte as ( #}
            select
                {% for value_col in value_cols %}
                {{value_col}},
                {% endfor %}
                1.0*( (row_number() over (order by {{value_cols[0]}} asc)) - 1 ) / count(*) over () as from_val,
                1.0*( (row_number() over (order by {{value_cols[0]}} asc))     ) / count(*) over () as to_val
            from {% if do_ref %}{{ref(model_name)}}{% else %}{{model_name}}{% endif %}
            {% if filter|trim|length %}
            where {{filter}}
            {% endif %}
            order by from_val asc, to_val asc
        {# ) #}
    {% endset %}
    {{ dbt_synth_data.synth_store("ctes", name+"__cte", cte) }}
    {% set cleanup_hook = "drop table if exists " ~ table_name ~ "__" ~ name ~ "__cte" %}
    {{ synth_add_cleanup_hook(cleanup_hook) or "" }}

    {% set base_field %}
        {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0, max=1) }} as {{name}}__rand
    {% endset %}
    {{ dbt_synth_data.synth_store("base_fields", name+"__rand", base_field) }}

    {% set join_fields %}
        {% if value_cols|length==1 %}
            {{table_alias}}__{{name}}__cte.{{value_cols[0]}} as {{name}}
        {% else %}
            {% for value_col in value_cols %}
            {{table_alias}}__{{name}}__cte.{{value_col}} as {{name}}__{{value_col}}
            {% if not loop.last %},{% endif %}
            {% endfor%}
        {% endif %}
    {% endset %}
    {% set join_clause %}
        left join {{table_name}}__{{name}}__cte on ___PREVIOUS_CTE___.{{name}}__rand between {{table_alias}}__{{name}}__cte.from_val and {{table_alias}}__{{name}}__cte.to_val
    {% endset %}
    {{ dbt_synth_data.synth_store("joins", name+"__cte", {"fields": join_fields, "clause": join_clause} ) }}

    {% set uniform_field %}
        {% if value_cols|length==1 %}
            {{name}}
        {% else %}
            {% for value_col in value_cols %}
            {{name}}__{{value_col}}
            {% if not loop.last %},{% endif %}
            {% endfor %}
        {% endif %}
    {% endset %}

    {% set final_field %}
        (
        CASE
            WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
            THEN NULL
            ELSE {{ uniform_field }}
        END
        ) as {{ name }}
    {% endset %}

    {{ dbt_synth_data.synth_store("final_fields", name, final_field) }}
{% endmacro %}

{% macro synth_column_select_weighted(name, model_name, value_cols, weight_col, filter, do_ref, null_frac) %}
    {{ return(adapter.dispatch('synth_column_select_weighted')(name, model_name, value_cols, weight_col, filter, do_ref, null_frac)) }}
{% endmacro %}

{% macro default__synth_column_select_weighted(name, model_name, value_cols, weight_col, filter, do_ref, null_frac) %}
    {% set table_name = dbt_synth_data.synth_retrieve('synth_conf')['table_name'] or "synth_table" %}
    {%- set frame_clause -%}
        {%- if target.type == "redshift" -%}rows between unbounded preceding and current row{%- endif -%}
    {%- endset -%}
    {% if not weight_col %}
        {{ exceptions.raise_compiler_error("`weight_col` is required when `distribution` for select column `" ~ name ~ "` is `weighted`.") }}
    {% endif %}
    
    {% set cte %}
        {# {{table_name}}__{{name}}__cte as ( #}
            select
                {% for value_col in value_cols %}
                {{value_col}},
                {% endfor %}
                {{weight_col}},
                ( sum({{weight_col}}) over (order by {{weight_col}} desc, {{value_cols[0]}} asc {{ frame_clause }}) - {{weight_col}}) / sum({{weight_col}}) over () as from_val,
                ( sum({{weight_col}}) over (order by {{weight_col}} desc, {{value_cols[0]}} asc {{ frame_clause }})                 ) / sum({{weight_col}}) over () as to_val
            from {% if do_ref %}{{ref(model_name)}}{% else %}{{model_name}}{% endif %}
            {% if filter|trim|length %}
            where {{filter}}
            {% endif %}
            order by from_val asc, to_val asc
        {# ) #}
    {% endset %}
    {{ dbt_synth_data.synth_store("ctes", name+"__cte", cte) }}

    {% set base_field %}
      {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0, max=1) }} as {{name}}__rand
    {% endset %}
    {{ dbt_synth_data.synth_store("base_fields", name+"__rand", base_field) }}

    {% set join_fields %}
        {% if value_cols|length==1 %}
            {{table_name}}__{{name}}__cte.{{value_cols[0]}} as {{name}}
        {% else %}
            {% for value_col in value_cols %}
            {{table_name}}__{{name}}__cte.{{value_col}} as {{name}}__{{value_col}}
            {% if not loop.last %},{% endif %}
            {% endfor%}
        {% endif %}
    {% endset %}
    {% set join_clause %}
        left join {{table_name}}__{{name}}__cte on ___PREVIOUS_CTE___.{{name}}__rand between {{table_name}}__{{name}}__cte.from_val and {{table_name}}__{{name}}__cte.to_val
    {% endset %}
    {{ dbt_synth_data.synth_store("joins", name+"__cte", {"fields": join_fields, "clause": join_clause} ) }}
    
    {% set weighted_field %}
        {% if value_cols|length==1 %}
            {{name}}
        {% else %}
            {% for value_col in value_cols %}
            {{name}}__{{value_col}}
            {% if not loop.last %},{% endif %}
            {% endfor %}
        {% endif %}
    {% endset %}

    {% set final_field %}
        (
        CASE
            WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
            THEN NULL
            ELSE {{ weighted_field }}
        END
        ) as {{ name }}
    {% endset %}

    {{ dbt_synth_data.synth_store("final_fields", name, final_field) }}
{% endmacro %}

{% macro bigquery__synth_column_select_weighted(name, model_name, value_cols, weight_col, filter, do_ref, null_frac) %}
    {% set table_name = target.schema ~ "." ~ (model.name or "synth_table") %}
    {% set table_alias = model.name or "synth_table" %}

    {% if not weight_col %}
        {{ exceptions.raise_compiler_error("`weight_col` is required when `distribution` for select column `" ~ name ~ "` is `weighted`.") }}
    {% endif %}

    {% set cte %}
        {# {{table_name}}__{{name}}__cte as ( #}
            select
                {% for value_col in value_cols %}
                {{value_col}},
                {% endfor %}
                {{weight_col}},
                ( sum({{weight_col}}) over (order by {{weight_col}} desc, {{value_cols[0]}} asc) - {{weight_col}}) / sum({{weight_col}}) over () as from_val,
                ( sum({{weight_col}}) over (order by {{weight_col}} desc, {{value_cols[0]}} asc)                 ) / sum({{weight_col}}) over () as to_val
            from {% if do_ref %}{{ref(model_name)}}{% else %}{{model_name}}{% endif %}
            {% if filter|trim|length %}
            where {{filter}}
            {% endif %}
            order by from_val asc, to_val asc
        {# ) #}
    {% endset %}
    {{ dbt_synth_data.synth_store("ctes", name+"__cte", cte) }}
    {% set cleanup_hook = "drop table if exists " ~ table_name ~ "__" ~ name ~ "__cte" %}
    {{ synth_add_cleanup_hook(cleanup_hook) or "" }}

    {% set base_field %}
      {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0, max=1) }} as {{name}}__rand
    {% endset %}
    {{ dbt_synth_data.synth_store("base_fields", name+"__rand", base_field) }}

    {% set join_fields %}
        {% if value_cols|length==1 %}
            {{table_alias}}__{{name}}__cte.{{value_cols[0]}} as {{name}}
        {% else %}
            {% for value_col in value_cols %}
            {{table_alias}}__{{name}}__cte.{{value_col}} as {{name}}__{{value_col}}
            {% if not loop.last %},{% endif %}
            {% endfor%}
        {% endif %}
    {% endset %}
    {% set join_clause %}
        left join {{table_name}}__{{name}}__cte on ___PREVIOUS_CTE___.{{name}}__rand between {{table_alias}}__{{name}}__cte.from_val and {{table_alias}}__{{name}}__cte.to_val
    {% endset %}
    {{ dbt_synth_data.synth_store("joins", name+"__cte", {"fields": join_fields, "clause": join_clause} ) }}

    {% set weighted_field %}
        {% if value_cols|length==1 %}
            {{name}}
        {% else %}
            {% for value_col in value_cols %}
            {{name}}__{{value_col}}
            {% if not loop.last %},{% endif %}
            {% endfor %}
        {% endif %}
    {% endset %}

    {% set final_field %}
        (
        CASE
            WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
            THEN NULL
            ELSE {{ weighted_field }}
        END
        ) as {{ name }}
    {% endset %}

    {{ dbt_synth_data.synth_store("final_fields", name, final_field) }}
{% endmacro %}