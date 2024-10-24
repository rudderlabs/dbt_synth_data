{% macro synth_distribution_continuous_normal(mean=0, stddev=1) -%}
    {{ return(adapter.dispatch('synth_distribution_continuous_normal')(mean, stddev)) }}
{%- endmacro %}

{% macro default__synth_distribution_continuous_normal(mean, stddev) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro sqlite__synth_distribution_continuous_normal(mean, stddev) %}
    {#- formula below is based on https://mathworld.wolfram.com/Box-MullerTransformation.html -#}
    ( ( 1.0*{{stddev}} * sqrt(-2*log({{ dbt_synth_data.synth_sqlite_random() }}))*sin(2*pi()*{{ dbt_synth_data.synth_sqlite_random() }}) ) + 1.0*{{mean}} )
{% endmacro %}

{% macro duckdb__synth_distribution_continuous_normal(mean, stddev) %}
    {#- formula below is based on https://mathworld.wolfram.com/Box-MullerTransformation.html -#}
    ( ( {{stddev}}::float * sqrt(-2*log(random()))*sin(2*pi()*random()) ) + {{mean}}::float )
{% endmacro %}

{% macro postgres__synth_distribution_continuous_normal(mean, stddev) %}
    {#- formula below is based on https://mathworld.wolfram.com/Box-MullerTransformation.html -#}
    ( ( {{stddev}}::float * sqrt(-2*log(random()))*sin(2*pi()*random()) ) + {{mean}}::float )
{% endmacro %}

{% macro databricks__synth_distribution_continuous_normal(mean, stddev) %}
    {#- formula below is based on https://mathworld.wolfram.com/Box-MullerTransformation.html -#}
    ( ( {{stddev}}::float * sqrt(-2*log(random()))*sin(2*pi()*random()) ) + {{mean}}::float )
{% endmacro %}

{% macro bigquery__synth_distribution_continuous_normal(mean, stddev) %}
    {#- formula below is based on https://mathworld.wolfram.com/Box-MullerTransformation.html -#}
    ( ( cast({{stddev}} as float64) * sqrt(-2*log(rand()))*sin(2*bqutil.fn.pi()*rand()) ) + cast({{mean}} as float64) )
{% endmacro %}

{% macro redshift__synth_distribution_continuous_normal(mean, stddev) %}
    {#- formula below is based on https://mathworld.wolfram.com/Box-MullerTransformation.html -#}
    ( ( {{stddev}}::float * sqrt(-2*log(random()))*sin(2*pi()*random()) ) + {{mean}}::float )
{% endmacro %}

{% macro snowflake__synth_distribution_continuous_normal(mean, stddev) %}
    NORMAL({{mean}}::float, {{stddev}}::float, RANDOM( {{ dbt_synth_data.synth_get_randseed() }} ))
{% endmacro %}