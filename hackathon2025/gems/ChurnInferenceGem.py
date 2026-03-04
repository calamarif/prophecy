import dataclasses
import json
from dataclasses import dataclass, field
from typing import List

from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


class ChurnInferenceGem(MacroSpec):
    name: str = "ChurnInferenceGem"
    projectName: str = "Hackathon_Telco_Churn"
    category: str = "Transform"
    minNumOfInputPorts: int = 1
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
    ]
    dependsOnUpstreamSchema: bool = True

    @dataclass(frozen=True)
    class ChurnInferenceGemProperties(MacroProperties):
        # Internal framework fields — not shown in the dialog
        schema: str = ""
        relation_name: List[str] = field(default_factory=list)
        # User-facing properties
        selectAllFeatures: bool = False
        featureColumns: List[str] = field(default_factory=list)
        churnThreshold: str = "0.5"
        lowMedThreshold: str = "0.3"
        medHighThreshold: str = "0.6"

    def get_relation_names(self, component: Component, context: SqlContext):
        all_upstream_nodes = []
        for inputPort in component.ports.inputs:
            upstreamNode = None
            for connection in context.graph.connections:
                if connection.targetPort == inputPort.id:
                    upstreamNodeId = connection.source
                    upstreamNode = context.graph.nodes.get(upstreamNodeId)
            all_upstream_nodes.append(upstreamNode)

        relation_name = []
        for upstream_node in all_upstream_nodes:
            if upstream_node is None or upstream_node.label is None:
                relation_name.append("")
            else:
                relation_name.append(upstream_node.label)

        return relation_name

    def dialog(self) -> Dialog:
        return Dialog("ChurnInferenceGem").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%")
                            .addElement(TitleElement("Feature Columns"))
                            .addElement(
                                Checkbox("Use all input columns").bindProperty(
                                    "selectAllFeatures"
                                )
                            )
                            .addElement(
                                Condition()
                                .ifEqual(
                                    PropExpr("component.properties.selectAllFeatures"),
                                    BooleanExpr(False),
                                )
                                .then(
                                    SchemaColumnsDropdown(
                                        "Select columns to pass to the model",
                                        appearance="minimal",
                                    )
                                    .withMultipleSelection()
                                    .bindSchema("component.ports.inputs[0].schema")
                                    .bindProperty("featureColumns")
                                )
                            )
                        )
                    )
                )
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%")
                            .addElement(TitleElement("Churn Classification Threshold"))
                            .addElement(
                                TextBox("Churn threshold (0.0 – 1.0)", placeholder="0.5")
                                .bindProperty("churnThreshold")
                            )
                        )
                    )
                )
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%")
                            .addElement(TitleElement("Risk Tier Boundaries"))
                            .addElement(
                                TextBox("Low / Medium boundary", placeholder="0.3")
                                .bindProperty("lowMedThreshold")
                            )
                            .addElement(
                                TextBox("Medium / High boundary", placeholder="0.6")
                                .bindProperty("medHighThreshold")
                            )
                        )
                    )
                )
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super(ChurnInferenceGem, self).validate(context, component)

        if not component.properties.selectAllFeatures and len(component.properties.featureColumns) == 0:
            diagnostics.append(
                Diagnostic(
                    "component.properties.featureColumns",
                    "Please select at least one feature column.",
                    SeverityLevelEnum.Error,
                )
            )

        if len(component.properties.featureColumns) > 0:
            missing_cols = [
                c
                for c in component.properties.featureColumns
                if c not in component.properties.schema
            ]
            if missing_cols:
                diagnostics.append(
                    Diagnostic(
                        "component.properties.featureColumns",
                        f"Selected columns {missing_cols} are not present in the input schema.",
                        SeverityLevelEnum.Error,
                    )
                )

        try:
            threshold = float(component.properties.churnThreshold)
            if threshold < 0.0 or threshold > 1.0:
                diagnostics.append(
                    Diagnostic(
                        "component.properties.churnThreshold",
                        "Threshold must be between 0.0 and 1.0.",
                        SeverityLevelEnum.Error,
                    )
                )
        except (ValueError, TypeError):
            diagnostics.append(
                Diagnostic(
                    "component.properties.churnThreshold",
                    "Threshold must be a number between 0.0 and 1.0.",
                    SeverityLevelEnum.Error,
                )
            )

        try:
            low_med = float(component.properties.lowMedThreshold)
            med_high = float(component.properties.medHighThreshold)
            if not (0.0 < low_med < med_high < 1.0):
                diagnostics.append(
                    Diagnostic(
                        "component.properties.lowMedThreshold",
                        "Risk tier boundaries must satisfy 0 < Low/Med < Med/High < 1.",
                        SeverityLevelEnum.Error,
                    )
                )
        except (ValueError, TypeError):
            diagnostics.append(
                Diagnostic(
                    "component.properties.lowMedThreshold",
                    "Risk tier boundaries must be numbers between 0.0 and 1.0.",
                    SeverityLevelEnum.Error,
                )
            )

        return diagnostics

    def onChange(
        self, context: SqlContext, oldState: Component, newState: Component
    ) -> Component:
        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [
            {"name": f["name"], "dataType": f["dataType"]["type"]}
            for f in schema["fields"]
        ]
        relation_name = self.get_relation_names(newState, context)
        feature_cols = newState.properties.featureColumns
        if newState.properties.selectAllFeatures:
            feature_cols = [f["name"] for f in schema["fields"]]
        newProperties = dataclasses.replace(
            newState.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name,
            featureColumns=feature_cols,
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: ChurnInferenceGemProperties) -> str:
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            str(props.relation_name),
            props.schema,
            str(props.featureColumns),
            str(props.churnThreshold),
            str(props.lowMedThreshold),
            str(props.medHighThreshold),
        ]
        params = ",".join(arguments)
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)
        return ChurnInferenceGem.ChurnInferenceGemProperties(
            relation_name=json.loads(
                parametersMap.get("relation_name", "[]").replace("'", '"')
            ),
            schema=parametersMap.get("schema", ""),
            selectAllFeatures=parametersMap.get("selectAllFeatures", "false").lower() == "true",
            featureColumns=json.loads(
                parametersMap.get("featureColumns", "[]").replace("'", '"')
            ),
            churnThreshold=parametersMap.get("churnThreshold", "0.5"),
            lowMedThreshold=parametersMap.get("lowMedThreshold", "0.3"),
            medHighThreshold=parametersMap.get("medHighThreshold", "0.6"),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", json.dumps(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("selectAllFeatures", str(properties.selectAllFeatures).lower()),
                MacroParameter("featureColumns", json.dumps(properties.featureColumns)),
                MacroParameter("churnThreshold", str(properties.churnThreshold)),
                MacroParameter("lowMedThreshold", str(properties.lowMedThreshold)),
                MacroParameter("medHighThreshold", str(properties.medHighThreshold)),
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        schema = json.loads(str(component.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [
            {"name": f["name"], "dataType": f["dataType"]["type"]}
            for f in schema["fields"]
        ]
        relation_name = self.get_relation_names(component, context)
        feature_cols = component.properties.featureColumns
        if component.properties.selectAllFeatures:
            feature_cols = [f["name"] for f in schema["fields"]]
        newProperties = dataclasses.replace(
            component.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name,
            featureColumns=feature_cols,
        )
        return component.bindProperties(newProperties)

    def applyPython(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
        import pickle
        import pandas as pd

        model_path = "/Volumes/westpac/callum/callums_volume/churn_models/churn_model.pkl"

        with open(model_path, "rb") as f:
            model_package = pickle.load(f)

        model = model_package["model"]
        scaler = model_package["scaler"]
        feature_columns = model_package["feature_columns"]

        input_pdf = in0.toPandas()

        existing_cols = [c for c in feature_columns if c in input_pdf.columns]
        X = input_pdf[existing_cols].fillna(0)
        X_scaled = scaler.transform(X)

        y_pred = model.predict(X_scaled)
        y_prob = model.predict_proba(X_scaled)[:, 1]

        input_pdf["churn_prediction"] = y_pred
        input_pdf["churn_probability"] = y_prob
        low_med = float(self.props.lowMedThreshold)
        med_high = float(self.props.medHighThreshold)
        input_pdf["churn_risk"] = pd.cut(
            input_pdf["churn_probability"],
            bins=[0, low_med, med_high, 1.0],
            labels=["Low", "Medium", "High"],
        )

        output_cols = [
            "customer_id", "city", "tenure_in_months", "contract", "monthly_charge",
            "total_revenue", "total_services_count", "tenure_category", "age_group",
            "churn_prediction", "churn_probability", "churn_risk",
        ]
        final_cols = [c for c in output_cols if c in input_pdf.columns]
        return spark.createDataFrame(input_pdf[final_cols])
