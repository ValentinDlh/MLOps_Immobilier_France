from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime
import requests
import os
import json
import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump


my_immo_dag = DAG(
    dag_id='immo_dag',
    description='My Airflow MLOps project DAG',
    tags=['MLOps_project_airflow_datascientest'],
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0),
    }
)


# Task 1 function : récupération des prédictions à tracker
def get_predictions():
    
    departements = [75, 69, 13]

    cities_75 = [
    "PARIS 10E ARRONDISSEMENT",
    "PARIS 11E ARRONDISSEMENT",
    "PARIS 12E ARRONDISSEMENT",
    "PARIS 13E ARRONDISSEMENT",
    "PARIS 14E ARRONDISSEMENT",
    "PARIS 15E ARRONDISSEMENT",
    "PARIS 16E ARRONDISSEMENT",
    "PARIS 17E ARRONDISSEMENT",
    "PARIS 18E ARRONDISSEMENT",
    "PARIS 19E ARRONDISSEMENT",
    "PARIS 1ER ARRONDISSEMENT",
    "PARIS 20E ARRONDISSEMENT",
    "PARIS 2E ARRONDISSEMENT",
    "PARIS 3E ARRONDISSEMENT",
    "PARIS 4E ARRONDISSEMENT",
    "PARIS 5E ARRONDISSEMENT",
    "PARIS 6E ARRONDISSEMENT",
    "PARIS 7E ARRONDISSEMENT",
    "PARIS 8E ARRONDISSEMENT",
    "PARIS 9E ARRONDISSEMENT"
    ]
    
    cities_69 = [
    "LYON 1ER ARRONDISSEMENT",
    "LYON 2E ARRONDISSEMENT",
    "LYON 3E ARRONDISSEMENT",
    "LYON 4E ARRONDISSEMENT",
    "LYON 5E ARRONDISSEMENT",
    "LYON 6E ARRONDISSEMENT",
    "LYON 7E ARRONDISSEMENT",
    "LYON 8E ARRONDISSEMENT",
    "LYON 9E ARRONDISSEMENT"
    ]

    cities_13 = [
    "MARSEILLE 10E ARRONDISSEMENT",
    "MARSEILLE 11E ARRONDISSEMENT",
    "MARSEILLE 12E ARRONDISSEMENT",
    "MARSEILLE 13E ARRONDISSEMENT",
    "MARSEILLE 14E ARRONDISSEMENT",
    "MARSEILLE 15E ARRONDISSEMENT",
    "MARSEILLE 16E ARRONDISSEMENT",
    "MARSEILLE 1ER ARRONDISSEMENT",
    "MARSEILLE 2E ARRONDISSEMENT",
    "MARSEILLE 3E ARRONDISSEMENT",
    "MARSEILLE 4E ARRONDISSEMENT",
    "MARSEILLE 5E ARRONDISSEMENT",
    "MARSEILLE 6E ARRONDISSEMENT",
    "MARSEILLE 7E ARRONDISSEMENT",
    "MARSEILLE 8E ARRONDISSEMENT",
    "MARSEILLE 9E ARRONDISSEMENT"
    ]

    surfaces = [30, 50, 70, 90, 110, 130, 150, 170, 190]

    vefa = [True, False]

   