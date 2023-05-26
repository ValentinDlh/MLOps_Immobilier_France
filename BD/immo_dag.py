from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import pandas as pd
import import_new_dataset
import test_unitaire


my_immo_dag = DAG(
    dag_id='immo_dag',
    description='My Airflow MLOps project DAG',
    tags=['MLOps_project_airflow_datascientest'],
    schedule_interval='0 0 1 6,12 *',
    start_date=datetime(2023, 6, 1),
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0),
    }
)


# Task 1
def import_new_data_main():

    import_new_dataset.import_new_dataset()
    print("import zip done !")
    import_new_dataset.file_extract()
    print("extract npz done !")
    df = import_new_dataset.npz_to_df()

    most_recent_date_new = import_new_dataset.get_date_max_new_transactions(df)
    print(most_recent_date_new)
    most_recent_date_old = import_new_dataset.get_date_max_old_transactions(import_new_dataset.collection)
    print(most_recent_date_old)

    if most_recent_date_new > most_recent_date_old:
        new_transactions = df[df["date_transaction"] > most_recent_date_old]
        print(new_transactions.columns)
        new_data = import_new_dataset.batch_iris(new_transactions)
        print(new_data.columns)
        new_data_final = import_new_dataset.generate_new_transactions(new_data)
        print(new_data_final.columns)
        import_new_dataset.import_new_transactions(new_data_final)
        print("done !")


# Task 2
def get_all_metrics(task_instance):

    date_last_DB=test_unitaire.get_metrics(test_unitaire.filename)['date_last_DB']
    nb_ligne_bd_Transactions=test_unitaire.get_metrics(test_unitaire.filename)['nb_ligne_bd_Transactions']
    nb_ligne_bd_Tdb_Quartier=test_unitaire.get_metrics(test_unitaire.filename)['nb_ligne_bd_Tdb_Quartier']
    df_rqt=pd.DataFrame(test_unitaire.get_metrics(test_unitaire.filename)['df_rqt_test_transaction'])
    
    print(date_last_DB)
    
    task_instance.xcom_push(
        key="nb_ligne_bd_Transactions",
        value=nb_ligne_bd_Transactions
    )
    task_instance.xcom_push(
        key="nb_ligne_bd_Tdb_Quartier",
        value=nb_ligne_bd_Tdb_Quartier
    )
    task_instance.xcom_push(
        key="df_rqt",
        value=df_rqt
    )


# Task 3
def test_unitaires(task_instance):
    
    nb_ligne_bd_Transactions = task_instance.xcom_pull(
            key="nb_ligne_bd_Transactions",
            task_ids=['task_2']
        )
    
    nb_ligne_bd_Tdb_Quartier = task_instance.xcom_pull(
            key="nb_ligne_bd_Tdb_Quartier",
            task_ids=['task_2']
        )
    
    df_rqt = task_instance.xcom_pull(
            key="df_rqt",
            task_ids=['task_2']
        )
    
    d={}
    d['date_last_DB']=str(datetime.now())
    test_nb_ligne_bd_Transactions,d['nb_ligne_bd_Transactions']=test_unitaire.test_nb_ligne_BD(nb_ligne_bd_Transactions,'Transactions')
    test_nb_ligne_bd_Tdb_Quartier,d['nb_ligne_bd_Tdb_Quartier']=test_unitaire.test_nb_ligne_BD(nb_ligne_bd_Tdb_Quartier,'Tdb_Quartier')
    test_df_rqt_test_transaction,d['df_rqt_test_transaction']=test_unitaire.test_rqt(df_rqt,'Transactions')
    

    task_instance.xcom_push(
        key="d",
        value=d
    )
    task_instance.xcom_push(
        key="test_nb_ligne_bd_Transactions",
        value=test_nb_ligne_bd_Transactions
    )
    task_instance.xcom_push(
        key="test_nb_ligne_bd_Tdb_Quartier",
        value=test_nb_ligne_bd_Tdb_Quartier
    )
    task_instance.xcom_push(
        key="test_df_rqt_test_transaction",
        value=test_df_rqt_test_transaction
    )
    

# Task 4
def write_metrics_all(task_instance):
    
    d = task_instance.xcom_pull(
            key="d",
            task_ids=['task_3']
        )
    
    test_nb_ligne_bd_Transactions = task_instance.xcom_pull(
            key="test_nb_ligne_bd_Transactions",
            task_ids=['task_3']
        )
    
    test_nb_ligne_bd_Tdb_Quartier = task_instance.xcom_pull(
            key="test_nb_ligne_bd_Tdb_Quartier",
            task_ids=['task_3']
        )
    
    test_df_rqt_test_transaction = task_instance.xcom_pull(
            key="test_df_rqt_test_transaction",
            task_ids=['task_3']
        )
    #écrire les valeurs
    if (test_nb_ligne_bd_Transactions & test_nb_ligne_bd_Tdb_Quartier & test_df_rqt_test_transaction):
        test_unitaire.write_metric(d,test_unitaire.filename)


task_1 = PythonOperator(
    task_id='import_new_data',
    python_callable=import_new_dataset.import_new_data_main,
    dag=my_immo_dag
)

task_2 = PythonOperator(
    task_id='get_metrics_all',
    python_callable=test_unitaire.get_all_metrics,
    dag=my_immo_dag
)

task_3 = PythonOperator(
    task_id='test_unitaires',
    python_callable=test_unitaire.test_unitaires,
    dag=my_immo_dag
)

task_4 = PythonOperator(
    task_id='write_metrics_all',
    python_callable=test_unitaire.write_metrics_all,
    dag=my_immo_dag
)

task_1 >> task_2
task_2 >> task_3
task_3 >> task_4
