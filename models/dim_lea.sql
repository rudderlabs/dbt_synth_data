{{ config(materialized='table') }}
{{ dbt_synth.table(
    rows = var('num_leas'),
    columns = [
        dbt_synth.column_primary_key(name='k_lea'),
        dbt_synth.column_value(name='k_lea__parent', value='c3203005af9e98e33a2cd94f030a2a89'),
        dbt_synth.column_value(name='k_sea', value='c3203005af9e98e33a2cd94f030a2a89'),
        dbt_synth.column_values(name='tenant_code', values=var('tenant_codes')),
        dbt_synth.column_integer_sequence(name='lea_id', step=1, start=125),
        dbt_synth.column_value(name='lea_name', value='LEANameComingSoon'),
        dbt_synth.column_value(name='lea_short_name', value='LEAShortNameComingSoon'),
        dbt_synth.column_value(name='parent_lea_id', value=None),
        dbt_synth.column_value(name='lea_category', value='Independent'),
        dbt_synth.column_value(name='education_service_center_id', value=None),
        dbt_synth.column_value(name='operational_status', value=None),
        dbt_synth.column_value(name='charter_status', value=None),
        dbt_synth.column_value(name='address_type', value='Mailing'),
        dbt_synth.column_address(name='street_address', countries=['United States'], geo_regions=[var('state_code')], address_types=['house'], parts=['street_address']),
        dbt_synth.column_address(name='city', countries=['United States'], geo_regions=[var('state_code')], address_types=['house'], parts=['city']),
        dbt_synth.column_value(name='name_of_county', value=None),
        dbt_synth.column_address(name='state_code', countries=['United States'], geo_regions=[var('state_code')], address_types=['house'], parts=['geo_region']),
        dbt_synth.column_address(name='postal_code', countries=['United States'], geo_regions=[var('state_code')], address_types=['house'], parts=['postal_code']),
        dbt_synth.column_value(name='building_site_number', value=None),
        dbt_synth.column_value(name='locale', value=None),
        dbt_synth.column_value(name='congressional_district', value=None),
        dbt_synth.column_value(name='county_fips_code', value=None),
        dbt_synth.column_value(name='latitude', value=None),
        dbt_synth.column_value(name='longitude', value=None),
    ]
) }}

{{ dbt_synth.add_update_hook("""
    insert into {{this}} (
        k_lea,
        k_lea__parent,
        k_sea,
        tenant_code,
        lea_id,
        lea_name,
        lea_short_name,
        parent_lea_id,
        lea_category,
        education_service_center_id,
        operational_status,
        charter_status,
        address_type,
        street_address,
        city,
        name_of_county,
        state_code,
        postal_code,
        building_site_number,
        locale,
        congressional_district,
        county_fips_code,
        latitude,
        longitude
    ) values (
        'c3203005af9e98e33a2cd94f030a2a89',
        NULL,
        NULL,
        '""" + var('tenant_codes')[0] + """',
        123,
        'Some State SEA',
        'SSSEA',
        NULL,
        'Independent',
        NULL,
        NULL,
        NULL,
        'Mailing',
        '123 Main St.',
        'Anytown',
        NULL,
        '""" + var('state_code') + """',
        12345,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    )
""") or "" }}

{{ config(post_hook=dbt_synth.get_post_hooks())}}
