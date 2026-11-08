{#
  Par défaut, dbt colle le schéma du profil devant le schéma du dossier
  (ex : "analytics_marts"). On veut des noms simples : staging, reference, marts.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
