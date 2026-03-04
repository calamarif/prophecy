{% macro ChurnInferenceGem(relation_name, schema, featureColumns=[], churnThreshold=0.5, lowMedThreshold=0.3, medHighThreshold=0.6) -%}
    {{ return(adapter.dispatch('ChurnInferenceGem', 'Hackathon_Telco_Churn')(relation_name, schema, featureColumns, churnThreshold, lowMedThreshold, medHighThreshold)) }}
{% endmacro %}


{%- macro default__ChurnInferenceGem(relation_name, schema, featureColumns=[], churnThreshold=0.5, lowMedThreshold=0.3, medHighThreshold=0.6) -%}

    {%- set rel_list = relation_name
        if (relation_name is iterable and relation_name is not string)
        else [relation_name] -%}

    {%- set feat_cols = featureColumns
        if (featureColumns is iterable and featureColumns is not string)
        else [] -%}

    {%- set feat_cast_exprs = [] -%}
    {%- for c in feat_cols -%}
        {%- do feat_cast_exprs.append('cast(`' ~ c ~ '` as double)') -%}
    {%- endfor -%}

    with churn_predictions as (
        select
            t.*,
            churn_inference_predict(array({{ feat_cast_exprs | join(', ') }})) as churn_probability
        from {{ rel_list[0] }} t
    )
    select
        *,
        case
            when churn_probability >= cast({{ churnThreshold }} as double) then 1
            else 0
        end as churn_prediction,
        case
            when churn_probability < cast({{ lowMedThreshold }} as double) then 'Low'
            when churn_probability < cast({{ medHighThreshold }} as double) then 'Medium'
            else 'High'
        end as churn_risk
    from churn_predictions

{%- endmacro -%}


{%- macro snowflake__ChurnInferenceGem(relation_name, schema, featureColumns=[], churnThreshold=0.5, lowMedThreshold=0.3, medHighThreshold=0.6) -%}

    {%- set rel_list = relation_name
        if (relation_name is iterable and relation_name is not string)
        else [relation_name] -%}

    {%- set feat_cols = featureColumns
        if (featureColumns is iterable and featureColumns is not string)
        else [] -%}

    {%- set feat_cast_exprs = [] -%}
    {%- for c in feat_cols -%}
        {%- do feat_cast_exprs.append('try_cast("' ~ c ~ '" as double)') -%}
    {%- endfor -%}

    with churn_predictions as (
        select
            t.*,
            churn_inference_predict(array_construct({{ feat_cast_exprs | join(', ') }})) as churn_probability
        from {{ rel_list[0] }} t
    )
    select
        *,
        case
            when churn_probability >= {{ churnThreshold }} then 1
            else 0
        end as churn_prediction,
        case
            when churn_probability < {{ lowMedThreshold }} then 'Low'
            when churn_probability < {{ medHighThreshold }} then 'Medium'
            else 'High'
        end as churn_risk
    from churn_predictions

{%- endmacro -%}


{%- macro duckdb__ChurnInferenceGem(relation_name, schema, featureColumns=[], churnThreshold=0.5, lowMedThreshold=0.3, medHighThreshold=0.6) -%}

    {%- set rel_list = relation_name
        if (relation_name is iterable and relation_name is not string)
        else [relation_name] -%}

    {%- set feat_cols = featureColumns
        if (featureColumns is iterable and featureColumns is not string)
        else [] -%}

    {%- set feat_cast_exprs = [] -%}
    {%- for c in feat_cols -%}
        {%- do feat_cast_exprs.append('cast(' ~ prophecy_basics.quote_identifier(c) ~ ' as double)') -%}
    {%- endfor -%}

    with churn_predictions as (
        select
            t.*,
            churn_inference_predict(list_value({{ feat_cast_exprs | join(', ') }})) as churn_probability
        from {{ rel_list[0] }} as t
    )
    select
        *,
        case
            when churn_probability >= {{ churnThreshold }} then 1
            else 0
        end as churn_prediction,
        case
            when churn_probability < {{ lowMedThreshold }} then 'Low'
            when churn_probability < {{ medHighThreshold }} then 'Medium'
            else 'High'
        end as churn_risk
    from churn_predictions

{%- endmacro -%}
