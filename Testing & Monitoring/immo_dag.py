from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import pandas as pd
import datetime




my_immo_dag = DAG(
    dag_id='immo_dag',
    description='My Airflow MLOps project DAG',
    tags=['MLOps_project_airflow_datascientest'],
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0),
    }
)


#task 1_0: test connexion BD

def test_connexion_bd():
    import db

    a=db.get_data_from_db({'id_transaction':107332}, 'Transactions')[0]['NOM_COM']
    print(a)

    return a



# Task 1_1
def import_new_data_main():
    import import_new_dataset
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

#Task1_2 : générer Tdb_quartier

def Tdb_ville():

    import db
    db.generer_tdb_quartier()


# Task 2 : récupérer les metriques dans le fichier json
def get_all_metrics(task_instance):
    import test_unitaire

    file_path = "./data/metrics.json"

    d = {}


    date_last_DB = test_unitaire.get_metrics(file_path)['date_last_DB']
    nb_ligne_bd_Transactions = test_unitaire.get_metrics(file_path)['nb_ligne_bd_Transactions']
    nb_ligne_bd_Tdb_Quartier = test_unitaire.get_metrics(file_path)['nb_ligne_bd_Tdb_Quartier']
    df_rqt =pd.DataFrame(test_unitaire.get_metrics(file_path)['df_rqt_test_transaction'])

    print(date_last_DB)
    d['date_last_DB']=date_last_DB
    d['nb_ligne_bd_Transactions'] = nb_ligne_bd_Transactions
    d['nb_ligne_bd_Tdb_Quartier']=nb_ligne_bd_Tdb_Quartier
    d['df_rqt_test_transaction']=df_rqt

    task_instance.xcom_push(
        key="metrics_import",
        value=d
    )


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
    print(nb_ligne_bd_Transactions)




# Task 3
def test_unitaires(task_instance):
    import test_unitaire

    metric = task_instance.xcom_pull(
        key="metrics_import",
        task_ids=['get_metrics_all']
    )

    nb_ligne_bd_Transactions = task_instance.xcom_pull(
        key="nb_ligne_bd_Transactions",
        task_ids=['get_metrics_all']
    )

    nb_ligne_bd_Tdb_Quartier = task_instance.xcom_pull(
        key="nb_ligne_bd_Tdb_Quartier",
        task_ids=['get_metrics_all']
    )

    df_rqt = task_instance.xcom_pull(
        key="df_rqt",
        task_ids=['get_metrics_all']
    )

    print('nb_ligne_bd_Transactions')
    print(nb_ligne_bd_Transactions)


    d = {}
    d['date_last_DB'] = str(datetime.datetime.now())

    test_nb_ligne_bd_Transactions, d['nb_ligne_bd_Transactions'] = test_unitaire.test_nb_ligne_BD(
        nb_ligne_bd_Transactions[0], 'Transactions')
    test_nb_ligne_bd_Tdb_Quartier, d['nb_ligne_bd_Tdb_Quartier'] = test_unitaire.test_nb_ligne_BD(
        nb_ligne_bd_Tdb_Quartier[0], 'Tdb_Quartier')
    test_df_rqt_test_transaction, d['df_rqt_test_transaction'] = test_unitaire.test_rqt(df_rqt[0], 'Transactions')

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
    import test_unitaire
    filename = "./data/metrics.json"

    d = task_instance.xcom_pull(
        key="d",
        task_ids=['test_unitaires']
    )[0]


    test_nb_ligne_bd_Transactions = task_instance.xcom_pull(
        key="test_nb_ligne_bd_Transactions",
        task_ids=['test_unitaires']
    )[0]

    test_nb_ligne_bd_Tdb_Quartier = task_instance.xcom_pull(
        key="test_nb_ligne_bd_Tdb_Quartier",
        task_ids=['test_unitaires']
    )[0]

    test_df_rqt_test_transaction = task_instance.xcom_pull(
        key="test_df_rqt_test_transaction",
        task_ids=['test_unitaires']
    )[0]

    # écrire les valeurs
    if (test_nb_ligne_bd_Transactions & test_nb_ligne_bd_Tdb_Quartier & test_df_rqt_test_transaction):
        test_unitaire.write_metric(d, filename)
        print('MAJ du fichier metrics.json')


task_1_0 = PythonOperator(
    task_id='test_connection_BD',
    python_callable=test_connexion_bd,
    dag=my_immo_dag,
)

'''
task_1_1 = PythonOperator(
    task_id='import_new_data',
    python_callable=import_new_data_main,
    dag=my_immo_dag
)


task_1_2 = PythonOperator(
    task_id='generer_tdb_quartier',
    python_callable=Tdb_ville,
    dag=my_immo_dag
)
'''

task_2 = PythonOperator(
    task_id='get_metrics_all',
    python_callable=get_all_metrics,
    dag=my_immo_dag
)

task_3 = PythonOperator(
    task_id='test_unitaires',
    python_callable=test_unitaires,
    dag=my_immo_dag
)

task_4 = PythonOperator(
    task_id='write_metrics_all',
    python_callable=write_metrics_all,
    dag=my_immo_dag
)

#task_1_0 >> task_1_1
#task_1_1 >> task_1_2
task_1_0>> task_2
#task_1_2 >> task_2
task_2 >> task_3
task_3 >> task_4
