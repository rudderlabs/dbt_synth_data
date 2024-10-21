{% macro synth_column_unique_key(name, null_frac=0) -%}
    {% set unique_field %}
        {{ adapter.dispatch('synth_column_unique_key')() }}
    {% endset %}

    {% set base_field %}
        CASE
            WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{null_frac}}
            THEN NULL
            ELSE {{ unique_field }}
        END as {{name}}
    {% endset %}

    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}

{% macro default__synth_column_unique_key() -%}
    MD5( __row_number::varchar )
{%- endmacro %}

{% macro sqlite__synth_column_unique_key() %}
    {# SQLite doesn't support MD5() out-of-the-box, so just use the row number #}
    __row_number
{% endmacro %}

{% macro snowflake__synth_column_unique_key() -%}
    UUID_STRING()
{%- endmacro %}

{% macro redshift__synth_column_unique_key() -%}
    LPAD(TO_HEX(FLOOR(RANDOM() * (2^32 - 1))::bigint), 8, '0') || '-' ||
    LPAD(TO_HEX(FLOOR(RANDOM() * (2^16 - 1))::bigint), 4, '0') || '-4' ||
    LPAD(TO_HEX(FLOOR(RANDOM() * (2^12 - 1))::bigint), 3, '0') || '-' ||
    LPAD(TO_HEX(FLOOR(RANDOM() * (2^16 - 1))::bigint), 4, '0') || '-' ||
    LPAD(TO_HEX(FLOOR(RANDOM() * (2^48 - 1))::bigint), 12, '0')
{%- endmacro %}

{% macro bigquery__synth_column_unique_key() -%}
    CONCAT(
        LOWER(FORMAT('%08X', CAST(FLOOR(RAND() * (POW(2, 32) - 1)) AS INT64))), '-',
        LOWER(FORMAT('%04X', CAST(FLOOR(RAND() * (POW(2, 16) - 1)) AS INT64))), '-4',
        LOWER(FORMAT('%03X', CAST(FLOOR(RAND() * (POW(2, 12) - 1)) AS INT64))), '-',
        LOWER(FORMAT('%04X', CAST(FLOOR(RAND() * (POW(2, 16) - 1)) AS INT64))), '-',
        LOWER(FORMAT('%012X', CAST(FLOOR(RAND() * (POW(2, 48) - 1)) AS INT64)))
    )
{%- endmacro %}

{% macro databricks__synth_column_unique_key() -%}
    UUID()
{%- endmacro %}