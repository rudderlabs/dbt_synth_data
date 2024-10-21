{% macro synth_table(rows=1000) -%}
    {{ return(adapter.dispatch('synth_table')(rows)) }}
{% endmacro %}

{% macro default__synth_table(rows=1000) -%}

    {# Load CTE name (to support multiple synth CTEs in one model) #}
    {% set table_name = dbt_synth_data.synth_retrieve('synth_conf')['table_name'] or "synth_table" %}

    {% set ctes = dbt_synth_data.synth_retrieve('ctes') %}
    {# {{ ctes.values() | list | join(",") }} #}
    {% for name, cte in ctes.items() %}
        {{table_name}}__{{name}} as (
            {{cte}}
        ) {% if ctes|length > 0 %} , {% endif %}
    {% endfor %}
    
    {{table_name}}__base as (
        select
            {{ adapter.dispatch('synth_table_rownum')() }} as __row_number
        from {{ adapter.dispatch('synth_table_generator')(rows) }}
    ),
    {{table_name}}__join0 as (
        select
            {{table_name}}__base.__row_number
            {% set base_fields = dbt_synth_data.synth_retrieve('base_fields') %}
            {% if base_fields.values() | length > 0 %},{% endif %}
            {{ base_fields.values() | list | join(",") }}
        from {{table_name}}__base
    ),
    {% set joins = dbt_synth_data.synth_retrieve('joins').values() | list %}
    {% for counter in range(1,joins|length+1) %}
        {{table_name}}__join{{counter}} as (
            select
                {{table_name}}__join{{counter-1}}.*
                {% if joins[counter-1]['fields']|length>0 %},{% endif %}
                {{ joins[counter-1]['fields'] | replace("___PREVIOUS_CTE___", table_name+"__join"+(counter-1)|string) }}
            from {{table_name}}__join{{counter-1}}
                {{ joins[counter-1]['clause'] | replace("___PREVIOUS_CTE___", table_name+"__join"+(counter-1)|string) }}
        ),
    {% endfor %}
    {{table_name}} as (
        select
            {% set final_fields = dbt_synth_data.synth_retrieve('final_fields').values() | list %}
            {% for final_field in final_fields %}
                {{ final_field | replace("___PREVIOUS_CTE___", "join"+joins|length|string) }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from {{table_name}}__join{{joins|length}}
    )
    {{ config(post_hook=dbt_synth_data.synth_get_post_hooks())}}
{%- endmacro %}

{% macro sqlite__synth_table(rows=1000) %}

    {# Load CTE name (to support multiple synth CTEs in one model) #}
    {% set table_name = dbt_synth_data.synth_retrieve('synth_conf')['table_name'] or "synth_table" %}

    {% set ctes = dbt_synth_data.synth_retrieve('ctes') %}
    {% for name, cte in ctes.items() %}
        {% set query %}
        drop table if exists {{table_name}}__{{name}};
        {% endset %}
        {% do run_query(query) %}

        {% set query %}
        create temp table {{table_name}}__{{name}} as
            {{cte}}
        ;
        {% endset %}
        {% do run_query(query) %}
    {% endfor %}
    {# {{ ctes.values() | list | join(",") }}
    {% if ctes|length > 0%},{% endif %} #}

    {% set query %}
    drop table if exists {{table_name}}__base;
    {% endset %}
    {% do run_query(query) %}

    {% set query %}
    create temp table {{table_name}}__base as
        select
            {{ adapter.dispatch('synth_table_rownum')() }} as __row_number
        from {{ adapter.dispatch('synth_table_generator')(rows) }}
    ;
    {% endset %}
    {% do run_query(query) %}

    {% set query %}
    drop table if exists {{table_name}}__join0;
    {% endset %}
    {% do run_query(query) %}

    {% set query %}
    create temp table {{table_name}}__join0 as
        select
            {{table_name}}__base.__row_number
            {% set base_fields = dbt_synth_data.synth_retrieve('base_fields') %}
            {% if base_fields.values() | length > 0 %},{% endif %}
            {{ base_fields.values() | list | join(",") }}
        from {{table_name}}__base
    ;
    {% endset %}
    {% do run_query(query) %}

    {% set joins = dbt_synth_data.synth_retrieve('joins').values() | list %}
    {% for counter in range(1,joins|length+1) %}
        {% set query %}
        drop table if exists {{table_name}}__join{{counter}};
        {% endset %}
        {% do run_query(query) %}

        {% set query %}
        create temp table {{table_name}}__join{{counter}} as
            select
                {{table_name}}__join{{counter-1}}.*
                {% if joins[counter-1]['fields']|length>0 %},{% endif %}
                {{ joins[counter-1]['fields'] | replace("___PREVIOUS_CTE___", table_name+"__join"+(counter-1)|string) }}
            from {{table_name}}__join{{counter-1}}
                {{ joins[counter-1]['clause'] | replace("___PREVIOUS_CTE___", table_name+"__join"+(counter-1)|string) }}
        ;
        {% endset %}
        {% do run_query(query) %}
    {% endfor %}
    {{table_name}} as (
        select
            {% set final_fields = dbt_synth_data.synth_retrieve('final_fields').values() | list %}
            {% for final_field in final_fields %}
                {{ final_field | replace("___PREVIOUS_CTE___", "join"+joins|length|string) }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from {{table_name}}__join{{joins|length}}
    )
    {{ config(post_hook=dbt_synth_data.synth_get_post_hooks())}}
{% endmacro %}


{% macro default__synth_table_generator(rows) -%}
    generate_series( 1, {{rows}}, 1 ) as s
{%- endmacro %}

{% macro postgres__synth_table_generator(rows) %}
    generate_series( 1, {{rows}} ) as s(idx)
{% endmacro %}

{% macro snowflake__synth_table_generator(rows) %}
    table(generator( rowcount => {{rows}} ))
{% endmacro %}

{% macro databricks__synth_table_generator(rows) %}
    (SELECT explode(sequence(1, {{ rows }})))
{% endmacro %}

{% macro default__synth_table_rownum() -%}
    row_number() over (order by NULL)
{%- endmacro %}

{% macro postgres__synth_table_rownum() %}
    s.idx
{% endmacro %}

{% macro databricks__synth_table_rownum() -%}
    uuid()
{%- endmacro %}

{% macro redshift__synth_table(rows=1000) %}
    {# Load CTE name (to support multiple synth CTEs in one model) #}
    {% set table_name = dbt_synth_data.synth_retrieve('synth_conf')['table_name'] or "synth_table" %}

    {% set ctes = dbt_synth_data.synth_retrieve('ctes') %}
    {% for name, cte in ctes.items() %}
        {% set query %}
        drop table if exists {{table_name}}__{{name}};
        {% endset %}
        {% do run_query(query) %}

        {% set query %}
        create temp table {{table_name}}__{{name}} as
            {{cte}}
        ;
        {% endset %}
        {% do run_query(query) %}
    {% endfor %}
    {# {{ ctes.values() | list | join(",") }}
    {% if ctes|length > 0%},{% endif %} #}

    {% set query %}
    drop table if exists {{table_name}}__base;
    {% endset %}
    {% do run_query(query) %}

    {% set gen_cycles = (rows/50e6) | int %}
    {% set gen_fraction = (rows - 50e6 * gen_cycles) | int %}

    {% set query %}
    create temp table {{table_name}}__base (__row_number numeric)
    {% endset %}
    {% do run_query(query) %}

    {% for i in range(0, gen_cycles) %}
        {% set query %}
        insert into {{table_name}}__base
        with recursive cte(val_num) as
        (
            select 1 as val_num
            union all
            select val_num+1 as val_num
            from cte
            where val_num < 50e6
        )
        select val_num + 50e6 * {{ gen_cycles }} as __row_number from cte order by val_num
        {% endset %}
        {% do run_query(query) %}
    {% endfor %}

    {% set query %}
    insert into {{table_name}}__base
    with recursive cte(val_num) as
    (
        select 1 as val_num
        union all
        select val_num+1 as val_num
        from cte
        where val_num < {{ gen_fraction }}
    )
    select val_num + 50e6 + {{ gen_cycles }} as __row_number from cte order by val_num
    {% endset %}
    {% do run_query(query) %}


    {% set query %}
    drop table if exists {{table_name}}__join0;
    {% endset %}
    {% do run_query(query) %}

    {% set query %}
    create temp table {{table_name}}__join0 as
        select
            {{table_name}}__base.__row_number
            {% set base_fields = dbt_synth_data.synth_retrieve('base_fields') %}
            {% if base_fields.values() | length > 0 %},{% endif %}
            {{ base_fields.values() | list | join(",") }}
        from {{table_name}}__base
    ;
    {% endset %}
    {% do run_query(query) %}

    {% set joins = dbt_synth_data.synth_retrieve('joins').values() | list %}
    {% for counter in range(1,joins|length+1) %}
        {% set query %}
        drop table if exists {{table_name}}__join{{counter}};
        {% endset %}
        {% do run_query(query) %}

        {% set query %}
        create temp table {{table_name}}__join{{counter}} as
            select
                {{table_name}}__join{{counter-1}}.*
                {% if joins[counter-1]['fields']|length>0 %},{% endif %}
                {{ joins[counter-1]['fields'] | replace("___PREVIOUS_CTE___", table_name+"__join"+(counter-1)|string) }}
            from {{table_name}}__join{{counter-1}}
                {{ joins[counter-1]['clause'] | replace("___PREVIOUS_CTE___", table_name+"__join"+(counter-1)|string) }}
        ;
        {% endset %}
        {% do run_query(query) %}
    {% endfor %}

    {{table_name}} as (
        select
            {% set final_fields = dbt_synth_data.synth_retrieve('final_fields').values() | list %}
            {% for final_field in final_fields %}
                {{ final_field | replace("___PREVIOUS_CTE___", "join"+joins|length|string) }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from {{table_name}}__join{{joins|length}}
    )
    {{ config(post_hook=dbt_synth_data.synth_get_post_hooks())}}

{% endmacro %}

{% macro bigquery__synth_table(rows=1000) %}
    {# Load CTE name (to support multiple synth CTEs in one model) #}
    {% set table_name = target.schema ~ "." ~ (model.name or "synth_table") %}
    {% set table_alias = model.name or "synth_table" %}

    {% set tables_to_drop = [] %}

    {% set ctes = dbt_synth_data.synth_retrieve('ctes') %}
    {% for name, cte in ctes.items() %}
        {% set query %}
        drop table if exists {{table_name}}__{{name}};
        {% endset %}
        {% do run_query(query) %}
        {% do tables_to_drop.append(table_name ~ "__" ~ name) %}

        {% set query %}
        create table {{table_name}}__{{name}} as
            {{cte}}
        ;
        {% endset %}
        {% do run_query(query) %}
    {% endfor %}
    {# {{ ctes.values() | list | join(",") }}
    {% if ctes|length > 0%},{% endif %} #}

    {% set query %}
    drop table if exists {{table_name}}__base;
    {% endset %}
    {% do run_query(query) %}

    {% set cleanup_hook = "drop table if exists " ~ table_name ~ "__base" %}
    {{ synth_add_cleanup_hook(cleanup_hook) or "" }}

    {% set query %}
    create table {{table_name}}__base (__row_number int64);
    {% endset %}
    {% do run_query(query) %}

    {% set gen_cycles = (rows/1e6) | int %}
    {% set gen_fraction = (rows - 1e6 * gen_cycles) | int %}

    {% for i in range(0, gen_cycles) %}
        {% set query %}
        insert into {{table_name}}__base
        SELECT cast(id+1e6*{{ i }} as int64) FROM UNNEST(generate_array(1, 1e6)) as id;
        {% endset %}
        {% do run_query(query) %}
    {% endfor %}

    {% set query %}
    insert into {{table_name}}__base
    SELECT cast(id+1e6*{{ gen_cycles }} as int64) FROM UNNEST(generate_array(1, {{ gen_fraction }})) as id;
    {% endset %}
    {% do run_query(query) %}

    {% set query %}
    drop table if exists {{table_name}}__join0;
    {% endset %}
    {% do run_query(query) %}

    {% set cleanup_hook = "drop table if exists " ~ table_name ~ "__join0" %}
    {{ synth_add_cleanup_hook(cleanup_hook) or "" }}

    {% set query %}
    create table {{table_name}}__join0 as
        select
            __row_number
            {% set base_fields = dbt_synth_data.synth_retrieve('base_fields') %}
            {% if base_fields.values() | length > 0 %},{% endif %}
            {{ base_fields.values() | list | join(",") }}
        from {{table_name}}__base
    ;
    {% endset %}
    {% do run_query(query) %}

    {% set joins = dbt_synth_data.synth_retrieve('joins').values() | list %}
    {% for counter in range(1,joins|length+1) %}
        {% set query %}
        drop table if exists {{table_name}}__join{{counter}};
        {% endset %}
        {% do run_query(query) %}

        {% set cleanup_hook = "drop table if exists " ~ table_name ~ "__join" ~ counter %}
        {{ synth_add_cleanup_hook(cleanup_hook) or "" }}

        {% set query %}
        create table {{table_name}}__join{{counter}} as
            select
                {{table_alias}}__join{{counter-1}}.*
                {% if joins[counter-1]['fields']|length>0 %},{% endif %}
                {{ joins[counter-1]['fields'] | replace("___PREVIOUS_CTE___", table_alias+"__join"+(counter-1)|string) }}
            from {{table_name}}__join{{counter-1}}
                {{ joins[counter-1]['clause'] | replace("___PREVIOUS_CTE___", table_alias+"__join"+(counter-1)|string) }}
        ;
        {% endset %}
        {% do run_query(query) %}
    {% endfor %}
    synth_table as (
        select
            {% set final_fields = dbt_synth_data.synth_retrieve('final_fields').values() | list %}
            {% for final_field in final_fields %}
                {{ final_field | replace("___PREVIOUS_CTE___", "join"+joins|length|string) }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from {{table_name}}__join{{joins|length}}
    )

    {# Cleanup: Drop the temporary tables #}
    {{ config(post_hook=dbt_synth_data.synth_get_post_hooks())}}

{% endmacro %}
