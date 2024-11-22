{% macro custom_calc(column_name) -%}
 ({{ column_name }} * 100 )
{%- endmacro %}