{# This Macro decode the categorical columns numeric to the original #}

{% macro decode_payment(payment_type) -%}
    case {{ payment_type }}
        when '1' then 'Credit card'
        when '2' then 'Cash'
        when '3' then 'No charge'
        when '4' then 'Dispute'
        when '5' then 'Unknown'
        when '6' then 'Voided trip'
    end
{%- endmacro %}
{% macro decode_ratecode(ratecodeid) -%}
    case {{ ratecodeid }}
        when '1' then 'Standard rate'
        when '2' then 'JFK'
        when '3' then 'Newark'
        when '4' then 'Nassau or Westchester'
        when '5' then 'Negotiated fare'
        when '6' then 'Group ride'
    end
{%- endmacro %}
{% macro decode_vendor(vendorid) -%}
    case {{ vendorid }}
        when '1' then 'Creative Mobile Technologies, LLC'
        when '2' then 'VeriFone Inc'
    end
{%- endmacro %}
{% macro decode_memory(flag_memory) -%}
    case {{ flag_memory }}
        when 'Y' then 'store and forward trip'
        when 'N' then 'not a store and forward trip'
    end
{%- endmacro %}