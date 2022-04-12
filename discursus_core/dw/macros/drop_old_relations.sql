{% macro drop_old_relations(dryrun=False) %}

{% if execute %}
  {% set current_models=[] %}

  {% for node in graph.nodes.values()
     | selectattr("resource_type", "in", ["model", "seed", "snapshot"])%}
    {% do current_models.append(node.name) %}

  {% endfor %}
{% endif %}

{% set cleanup_query %}

      WITH MODELS_TO_DROP AS (
        SELECT
          CASE 
            WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE'
            WHEN TABLE_TYPE = 'VIEW' THEN 'VIEW'
          END AS RELATION_TYPE,
          CONCAT_WS('.', TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) AS RELATION_NAME
        FROM 
          {{ target.database }}.INFORMATION_SCHEMA.TABLES
        WHERE lower(TABLE_SCHEMA) = lower('{{ target.schema }}')
          AND lower(TABLE_NAME) NOT IN
            ({%- for model in current_models -%}
                '{{ model.lower() }}'
                {%- if not loop.last -%}
                    ,
                {% endif %}
            {%- endfor -%}))
      SELECT 
        'DROP ' || RELATION_TYPE || ' ' || RELATION_NAME || ';' as DROP_COMMANDS
      FROM 
        MODELS_TO_DROP

  {% endset %}

{% do log(cleanup_query, info=True) %}
{% set drop_commands = run_query(cleanup_query).columns[0].values() %}

{% if drop_commands %}
  {% if dryrun | as_bool == False %}
    {% do log('Executing DROP commands...', True) %}
  {% else %}
    {% do log('Printing DROP commands...', True) %}
  {% endif %}
  {% for drop_command in drop_commands %}
    {% do log(drop_command, True) %}
    {% if dryrun | as_bool == False %}
      {% do run_query(drop_command) %}
    {% endif %}
  {% endfor %}
{% else %}
  {% do log('No relations to clean.', True) %}
{% endif %}

{%- endmacro -%}