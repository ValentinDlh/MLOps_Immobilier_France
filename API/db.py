import pandas as pd
import json
from bson.json_util import dumps
#import bson
import numpy as np
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

#from airflow.providers.http.operators.http import SimpleHttpOperator
#from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime,timedelta
from dotenv import load_dotenv
import os



load_dotenv()


#----/!\----
#dans repo .gitgnore
#dans gitgnore mettre le .env
#----/!\----

#----/!\----
#log.info plutot que print si pas d'affichage des print
#----/!\----



'''
def connecter():
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    database_immo = client['Immo']
    #print(client.server_info())
    return database_immo
'''

# Connection à la BD
#"mongodb+srv://vlago:DE_Immo_2023@cluster0.fv699mr.mongodb.net/?retryWrites=true&w=majority"

uri = os.getenv("DB_URI")#"mongodb+srv://vlago:DE_Immo_2023@cluster0.fv699mr.mongodb.net/"
client = MongoClient(uri, server_api=ServerApi('1'))
db=client['Immo']

collection = db['Transactions']


# Obtenir une transaction par son ID
def get_data_from_db(c,collec):

    #a=collection.find({"id_transaction": c})[0]['NOM_IRIS']
    #print(a)
    return db[collec].find(c)



def rqt_bd(rqt,col):
    #return db.db[collection].count_documents(rqt)
    return list(db[col].aggregate(rqt))



# extraire les données d'une requête dans la BD dans un dataframe

def extract_from_DB_to_df_with_condition(condition: dict, col):
    df = pd.DataFrame.from_dict(json.loads(dumps(collection.find(condition))))
    if df.empty :
        return None

    df.set_index(['id_transaction'])
    # transformation de la date en format datetime
    df["date_transaction"] = df.date_transaction.apply(lambda x: pd.to_datetime(x['$date'], errors='coerce'))

    # affectation du semestre de la transaction
    df['semester'] = df.date_transaction.dt.year.astype(str) + 'S' + np.where(df.date_transaction.dt.quarter.gt(2), 2,
                                                                              1).astype(str)

    # selection des colonnes d'intéret dans le dataframe
    df = df[col]

    return df


# extraction des valeurs unique de la BD sur la colonne v

def extract_distinct_value(condition: dict, v):
    return collection.find(condition).distinct(v)


def generer_tdb_quartier():
#création de la requête qui permettra de générer notre tableau de bord quartier à partir de la table Transactions
# SELECT departement,NOM_COM,ANNEE, AVG(Prix_m2),STD(Prix_m2),AVG(Prix_m2),COUNT(id_Transaction) FROM Transaction GROUP BY departement, NOM_COM,ANNEE ORDER BY ANNEE,departement,NOM_COM
    pipeline = [
        {
            '$match': {
                'surface_habitable': {'$gt': 0},
                'prix_m2': {'$gt': 500}
            }
        },
        {
            '$group': {
                '_id': {
                    'departement': '$departement',
                    'NOM_COM': '$NOM_COM',
                    #'NOM_IRIS':'$NOM_IRIS'
                    'ANNEE': {'$year': '$date_transaction'}
                },
                'Prix_moyen_m2': {'$avg': '$prix_m2'},
                'Prix_moyen_m2_ecart_type': {'$stdDevPop': '$prix_m2'},
                'AVG_surface_habitable': {'$avg': '$surface_habitable'},
                'COUNT_id_transaction': {'$sum': 1}
            }
        },
        {
            '$sort': {
                '_id.departement': 1,
                '_id.NOM_COM':1,
                '_id.ANNEE': 1,
            }
        },
        {
            "$project": {
                "_id": 0,
                "departement": "$_id.departement",
                "NOM_COM": "$_id.NOM_COM",
                "ANNEE": "$_id.ANNEE",
                "Prix_moyen_m2": {'$toInt': '$Prix_moyen_m2'},  # Conversion en entier
                "Prix_moyen_m2_ecart_type": {'$toInt': '$Prix_moyen_m2_ecart_type'},
                "Moyenne_surface_habitable": {'$toInt': '$AVG_surface_habitable'},
                'COUNT_id_transaction': 1
            }
        }
    ]

    result = db.Transactions.aggregate(pipeline)
    collection_list=db.list_collection_names()

    for c in collection_list:
        if c == 'Tdb_Quartier' :
            db['Tdb_Quartier'].drop()

    new_collection = db['Tdb_Quartier']

      # Nouvelle collection pour enregistrer les résultats

    documents_to_insert = list(result)

    if documents_to_insert:
        new_collection.insert_many(documents_to_insert)

def test_conn():
    try:
    # Création d'une instance du client MongoDB
        uri = "mongodb://localhost:27017"
        client = MongoClient(uri)
    # Sélection de la base de données
        db = client['Transactions']

    # Vérification de la connexion
        if db.name:
            print("Connexion réussie à la base de données :", db.name)
        # Effectuez d'autres opérations ici si nécessaire
        # ...

    # Fermeture de la connexion
        client.close()

    except Exception as e:
        print("Erreur lors de la connexion à la base de données :", str(e))
