{% macro column_word(name, distribution="prevalence", pos=[]) -%}
    {% set filter %}
        {% for p in pos %}
            types like '{{p}};%' OR types like '%;{{p}};%' OR types like '%;{{p}}'
            {% if not loop.last %}OR {% endif %}
        {% endfor %}
    {% endset %}
    {{ return(
        dbt_synth.column_select(
            name=name,
            value_col="word",
            lookup_table="synth_words",
            distribution=distribution,
            prevalence_col="prevalence",
            filter=filter,
            funcs=["INITCAP"]
        )
    ) }}
{%- endmacro %}