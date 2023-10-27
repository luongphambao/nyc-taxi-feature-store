from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(dag_id="python", start_date=datetime(2023, 7, 1), schedule=None) as dag:
    # This is often used for tasks which are more suitable for executing commands
    # For example, submit a job to a Spark cluster, initiate a new cluster,
    # run containers, upgrade software packages on Linux systems,
    # or installing a PyPI package
    system_maintenance_task = BashOperator(
        task_id="system_maintenance_task",
        # bash_command='apt-get update && apt-get upgrade -y'
        bash_command='echo "Install some pypi libs..."',
    )

    # https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html
    @task.virtualenv(
        task_id="virtualenv_python",
        requirements=["lightgbm==4.1.0", "pandas==2.0.3", "scikit-learn==1.3.1"],
        system_site_packages=False,
    )
    def train_task():
        # This example is edited from this source https://github.com/microsoft/LightGBM/blob/master/examples/python-guide/simple_example.py
        import os

        import lightgbm as lgb
        import pandas as pd
        from sklearn.metrics import mean_squared_error

        DATA_DIR = "/opt/airflow/data/regression"

        print("Loading data...")
        # load or create your dataset
        df_train = pd.read_csv(
            os.path.join(DATA_DIR, "regression.train"),
            header=None,
            sep="\t",
        )
        df_test = pd.read_csv(
            os.path.join(DATA_DIR, "regression.test"),
            header=None,
            sep="\t",
        )

        y_train = df_train[0]
        y_test = df_test[0]
        X_train = df_train.drop(0, axis=1)
        X_test = df_test.drop(0, axis=1)

        # create dataset for lightgbm
        lgb_train = lgb.Dataset(X_train, y_train)
        lgb_eval = lgb.Dataset(X_test, y_test, reference=lgb_train)

        # specify your configurations as a dict
        params = {
            "boosting_type": "gbdt",
            "objective": "regression",
            "metric": {"l2", "l1"},
            "num_leaves": 31,
            "learning_rate": 0.05,
            "feature_fraction": 0.9,
            "bagging_fraction": 0.8,
            "bagging_freq": 5,
            "verbose": 0,
        }

        print("Starting training...")
        # train
        gbm = lgb.train(
            params,
            lgb_train,
            num_boost_round=20,
            valid_sets=lgb_eval,
            callbacks=[lgb.early_stopping(stopping_rounds=5)],
        )

        print("Saving model...")
        # save model to file
        gbm.save_model(os.path.join("model.txt"))

        print("Starting predicting...")
        # predict
        y_pred = gbm.predict(X_test, num_iteration=gbm.best_iteration)
        # eval
        rmse_test = mean_squared_error(y_test, y_pred) ** 0.5
        print(f"The RMSE of prediction is: {rmse_test}")

    system_maintenance_task >> train_task()
