from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "inference_pipeline", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    inference_pipeline__join_1 = Process(
        name = "inference_pipeline__Join_1",
        properties = ModelTransform(modelName = "inference_pipeline__Join_1")
    )
    churn_summary_by_risk = Process(name = "churn_summary_by_risk", properties = Visualize(), output_ports = None)
    load_and_predict = Process(
        name = "load_and_predict",
        properties = Script(
          script = "import pickle\nimport os\nimport numpy as np\nimport pandas as pd\nfrom pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType\n\n# Load the trained model package from pickle\nmodel_path = \"/Volumes/westpac/callum/callums_volume/churn_models/churn_model.pkl\"\n\nprint(f\"Loading model from: {model_path}\")\nwith open(model_path, 'rb') as f:\n    model_package = pickle.load(f)\n\n# Extract model components\nmodel = model_package['model']\nscaler = model_package['scaler']\nfeature_columns = model_package['feature_columns']\n\nprint(f\"Model loaded successfully. Using {len(feature_columns)} features.\")\n\n# Convert Spark DataFrame to Pandas for prediction\ninput_pdf = in0.toPandas()\nprint(f\"Input data has {len(input_pdf)} rows\")\n\n# Filter to existing feature columns\nexisting_cols = [c for c in feature_columns if c in input_pdf.columns]\nprint(f\"Found {len(existing_cols)} of {len(feature_columns)} expected feature columns\")\n\n# Prepare features\nX = input_pdf[existing_cols].fillna(0)\n\n# Scale features using the saved scaler\nX_scaled = scaler.transform(X)\n\n# Make predictions\ny_pred = model.predict(X_scaled)\ny_prob = model.predict_proba(X_scaled)[:, 1]\n\n# Add predictions to the dataframe\ninput_pdf['churn_prediction'] = y_pred\ninput_pdf['churn_probability'] = y_prob\n\n# Create risk category based on probability\ninput_pdf['churn_risk'] = pd.cut(\n    input_pdf['churn_probability'],\n    bins=[0, 0.3, 0.6, 1.0],\n    labels=['Low', 'Medium', 'High']\n)\n\n# Select output columns - include key identifiers and predictions\noutput_cols = [\n    'customer_id', 'city', 'tenure_in_months', 'contract', 'monthly_charge',\n    'total_revenue', 'total_services_count', 'tenure_category', 'age_group',\n    'churn_prediction', 'churn_probability', 'churn_risk'\n]\n\n# Filter to columns that exist\nfinal_cols = [c for c in output_cols if c in input_pdf.columns]\noutput_pdf = input_pdf[final_cols]\n\n# Convert back to Spark DataFrame\nout0 = spark.createDataFrame(output_pdf)\n\nprint(f\"Predictions complete. {int(y_pred.sum())} customers predicted to churn ({100*y_pred.mean():.1f}%)\")",
          scriptMethodFooter = "return out0",
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:"
        )
    )
    churn_predictions_detail = Process(name = "churn_predictions_detail", properties = Visualize(), output_ports = None)
    load_and_predict._out(0) >> [churn_predictions_detail._in(0), churn_summary_by_risk._in(0)]
    inference_pipeline__join_1 >> load_and_predict
