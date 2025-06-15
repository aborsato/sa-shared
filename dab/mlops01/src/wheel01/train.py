import argparse

import mlflow
from mlflow.models import infer_signature

import pandas as pd
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

# Set the MLflow tracking URI to the Databricks Unity Catalog
mlflow.set_registry_uri('databricks-uc')

def train_and_load(experiment_name: str, catalog_name: str, schema_name: str, model_name: str):
    # Load the Iris dataset
    X, y = datasets.load_iris(return_X_y=True)

    # Split the data into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Define the model hyperparameters
    params = {
        "solver": "lbfgs",
        "max_iter": 100,
        "random_state": 8888,
    }

    # Train the model
    lr = LogisticRegression(**params)
    lr.fit(X_train, y_train)

    # Predict on the test set
    y_pred = lr.predict(X_test)

    # Calculate metrics
    accuracy = accuracy_score(y_test, y_pred)

    # Set our tracking server uri for logging
    # (Not needed: Databricks automatically sets the tracking server)
    # mlflow.set_tracking_uri(uri="http://127.0.0.1:8080")

    # Define MLflow Experiment
    mlflow.set_experiment(experiment_name)

    # Start an MLflow run
    with mlflow.start_run():
        # Log the hyperparameters
        mlflow.log_params(params)

        # Log the loss metric
        mlflow.log_metric("accuracy", accuracy)

        # Set a tag that we can use to remind ourselves what this run was for
        mlflow.set_tag("Training Info", "Basic LR model for iris data")

        # Infer the model signature
        signature = infer_signature(X_train, lr.predict(X_train))

        # Log the model
        model_info = mlflow.sklearn.log_model(
            sk_model=lr,
            artifact_path="iris_model",
            signature=signature,
            input_example=X_train,
            registered_model_name=f"{catalog_name}.{schema_name}.{model_name}"
        )

def main():
    parser = argparse.ArgumentParser(description="Run ML Model training and logging")
    parser.add_argument("--experiment", type=str, required=True, help="Experiment name")
    parser.add_argument("--catalog", type=str, required=True, help="Catalog name")
    parser.add_argument("--schema", type=str, required=False, help="Schema name", default="default")
    parser.add_argument("--model", type=str, required=True, help="Model name")
    args = parser.parse_args()

    train_and_load(
        experiment_name=args.experiment,
        catalog_name=args.catalog,
        schema_name=args.schema,
        model_name=args.model
    )

if __name__ == "__main__":
    main()
