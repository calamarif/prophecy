from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "model_training_pipeline",
    version = 1,
    auto_layout = False,
    params = Parameters(test_size = 0.2, random_state = 42, model_type = "'logistic_regression'")
)

with Pipeline(args) as pipeline:
    model_training_pipeline__fe_derived_features = Process(
        name = "model_training_pipeline__fe_derived_features",
        properties = ModelTransform(modelName = "model_training_pipeline__fe_derived_features")
    )
    metrics_visualization = Process(name = "metrics_visualization", properties = Visualize(), output_ports = None)
    save_model_to_file = Process(
        name = "save_model_to_file",
        properties = Script(
          script = "# Model save confirmation - actual save happens in churn_model_training\nprint(\"Model has been saved to: /Volumes/westpac/callum/callums_volume/churn_models/churn_model.pkl\")\nprint(\"This file is overwritten on each pipeline run.\")\n\n# Pass through the input for downstream processing\nout0 = in0",
          scriptMethodFooter = "return out0",
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:"
        )
    )
    churn_model_training = Process(
        name = "churn_model_training",
        properties = Script(
          script = "import pickle\nimport os\nimport numpy as np\nimport pandas as pd\nfrom pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType\nfrom sklearn.linear_model import LogisticRegression\nfrom sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score\nfrom sklearn.preprocessing import StandardScaler\n\n# Define feature columns (numeric features for the model)\nfeature_cols = [\n    'age_numeric', 'tenure_in_months', 'number_of_referrals',\n    'number_of_dependents', 'monthly_charge', 'total_charges',\n    'total_revenue', 'avg_monthly_long_distance_charges',\n    'avg_monthly_gb_download', 'total_refunds', 'total_extra_data_charges',\n    'total_long_distance_charges', 'is_married', 'has_phone_service',\n    'has_internet_service', 'has_online_security', 'has_online_backup',\n    'has_device_protection', 'has_premium_tech_support', 'has_streaming_tv',\n    'has_streaming_movies', 'has_streaming_music', 'has_unlimited_data',\n    'has_paperless_billing', 'contract_type_encoded', 'payment_method_encoded',\n    'internet_type_encoded'\n]\n\n# Convert Spark DataFrames to Pandas\ntrain_pdf = in0.toPandas()\ntest_pdf = in1.toPandas()\n\n# Filter to only existing columns\nexisting_cols = [c for c in feature_cols if c in train_pdf.columns]\nprint(f\"Using {len(existing_cols)} feature columns\")\n\n# Prepare training data\nX_train = train_pdf[existing_cols].fillna(0)\ny_train = train_pdf['churn_label'].fillna(0)\n\n# Prepare test data\nX_test = test_pdf[existing_cols].fillna(0)\ny_test = test_pdf['churn_label'].fillna(0)\n\n# Scale features\nscaler = StandardScaler()\nX_train_scaled = scaler.fit_transform(X_train)\nX_test_scaled = scaler.transform(X_test)\n\n# Train logistic regression model\nlr = LogisticRegression(max_iter=100, C=100, random_state=42)\nlr.fit(X_train_scaled, y_train)\n\n# Create model package with scaler and feature columns\nmodel_package = {\n    'model': lr,\n    'scaler': scaler,\n    'feature_columns': existing_cols\n}\n\n# Save model as pickle file (overwrites existing file)\nmodel_dir = \"/Volumes/westpac/callum/callums_volume/churn_models\"\nmodel_path = f\"{model_dir}/churn_model.pkl\"\n\n# Create directory if it doesn't exist\nos.makedirs(model_dir, exist_ok=True)\n\n# Save model using pickle (overwrite mode)\nwith open(model_path, 'wb') as f:\n    pickle.dump(model_package, f)\nprint(f\"Model saved to: {model_path}\")\n\n# Make predictions on test set\ny_pred = lr.predict(X_test_scaled)\ny_prob = lr.predict_proba(X_test_scaled)[:, 1]\n\n# Calculate metrics\naccuracy = accuracy_score(y_test, y_pred)\nprecision = precision_score(y_test, y_pred, average='weighted', zero_division=0)\nrecall = recall_score(y_test, y_pred, average='weighted', zero_division=0)\nf1 = f1_score(y_test, y_pred, average='weighted', zero_division=0)\ntry:\n    auc_roc = roc_auc_score(y_test, y_prob)\nexcept:\n    auc_roc = 0.0\n\nprint(f\"AUC-ROC: {auc_roc:.4f}\")\nprint(f\"Accuracy: {accuracy:.4f}\")\nprint(f\"Precision: {precision:.4f}\")\nprint(f\"Recall: {recall:.4f}\")\nprint(f\"F1 Score: {f1:.4f}\")\n\n# Create metrics DataFrame\nmetrics_schema = StructType([\n    StructField(\"metric_name\", StringType(), True),\n    StructField(\"metric_value\", DoubleType(), True)\n])\n\nmetrics_data = [\n    (\"AUC-ROC\", float(auc_roc)),\n    (\"Accuracy\", float(accuracy)),\n    (\"Precision\", float(precision)),\n    (\"Recall\", float(recall)),\n    (\"F1 Score\", float(f1))\n]\n\nmetrics_df = spark.createDataFrame(metrics_data, metrics_schema)\n\n# Create model output DataFrame with predictions\ntest_pdf['prediction'] = y_pred\ntest_pdf['probability'] = y_prob\noutput_cols = ['customer_id', 'churn_label', 'prediction', 'probability'] if 'customer_id' in test_pdf.columns else ['churn_label', 'prediction', 'probability']\nmodel_output_df = spark.createDataFrame(test_pdf[output_cols])\n\n# Assign output DataFrames - out0 goes to metrics formatter, out1 goes to model saver\nout0 = metrics_df\nout1 = model_output_df\n\nprint(f\"Model training completed successfully.\")",
          scriptMethodFooter = "return (out0, out1)",
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> (DataFrame, DataFrame):"
        ),
        input_ports = ["train_in", "test_in"],
        output_ports = ["model_out", "metrics_out"]
    )
    model_training_pipeline__fm_formatted_metrics = Process(
        name = "model_training_pipeline__fm_formatted_metrics",
        properties = ModelTransform(modelName = "model_training_pipeline__fm_formatted_metrics")
    )
    train_test_split = Process(
        name = "train_test_split",
        properties = Script(
          script = "# Train/Test Split using randomSplit\n# Configurable test_size via pipeline parameter\ntest_size = 0.2  # Can be parameterized\ntrain_size = 1.0 - test_size\n\n# Split the data (no cache - serverless compute doesn't support PERSIST)\ntrain_df, test_df = in0.randomSplit([train_size, test_size], seed=42)\n\nprint(f\"Train/Test split completed with {test_size*100}% test size\")\n\n# Assign outputs\nout0 = train_df\nout1 = test_df",
          scriptMethodFooter = "return (out0, out1)",
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> (DataFrame, DataFrame):"
        ),
        output_ports = ["train_out", "test_out"]
    )
    train_test_split._out(0) >> churn_model_training._in(0)
    train_test_split._out(1) >> churn_model_training._in(1)
    churn_model_training._out(0) >> [save_model_to_file._in(0), model_training_pipeline__fm_formatted_metrics._in(0)]
    model_training_pipeline__fm_formatted_metrics >> metrics_visualization
    model_training_pipeline__fe_derived_features >> train_test_split
