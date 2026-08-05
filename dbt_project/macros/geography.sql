{#
    Map a Brazilian state abbreviation to its IBGE macro-region.

    Lives in a macro rather than a CASE pasted into three models so the
    dashboard's "revenue by region" slice has exactly one definition.
#}
{% macro brazil_region(state_column) -%}
    case upper(trim({{ state_column }}))
        when 'AC' then 'Norte'
        when 'AP' then 'Norte'
        when 'AM' then 'Norte'
        when 'PA' then 'Norte'
        when 'RO' then 'Norte'
        when 'RR' then 'Norte'
        when 'TO' then 'Norte'
        when 'AL' then 'Nordeste'
        when 'BA' then 'Nordeste'
        when 'CE' then 'Nordeste'
        when 'MA' then 'Nordeste'
        when 'PB' then 'Nordeste'
        when 'PE' then 'Nordeste'
        when 'PI' then 'Nordeste'
        when 'RN' then 'Nordeste'
        when 'SE' then 'Nordeste'
        when 'DF' then 'Centro-Oeste'
        when 'GO' then 'Centro-Oeste'
        when 'MT' then 'Centro-Oeste'
        when 'MS' then 'Centro-Oeste'
        when 'ES' then 'Sudeste'
        when 'MG' then 'Sudeste'
        when 'RJ' then 'Sudeste'
        when 'SP' then 'Sudeste'
        when 'PR' then 'Sul'
        when 'RS' then 'Sul'
        when 'SC' then 'Sul'
        else 'Unknown'
    end
{%- endmacro %}


{#
    Brazil's rough bounding box. Used to quarantine impossible geolocation
    points rather than letting them drag a map's centroid into the ocean.
#}
{% macro in_brazil_bounds(lat_column, lng_column) -%}
    (
        {{ lat_column }} between -33.75 and 5.27
        and {{ lng_column }} between -73.99 and -34.79
    )
{%- endmacro %}
