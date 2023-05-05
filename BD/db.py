import pandas as pd
import datetime
import pymongo
import json
from bson.json_util import dumps
import numpy as np
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

#Connection à la BD
uri="mongodb+srv://vlago:DE_Immo_2023@cluster0.fv699mr.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri, server_api=ServerApi('1'))
db=client['Immo']
collection=db['Transactions']


# Obtenir une transaction par son ID
def get_one(c:str):
    return collection.find_one({"_id": c})

# extraire les données d'une requête dans la BD dans un dataframe

def extract_from_DB_to_df_with_condition(condition:dict,col):

    df = pd.DataFrame.from_dict(json.loads(dumps(collection.find(condition))))
    df.set_index(['id_transaction'])
    #transformation de la date en format datetime
    df["date_transaction"] = df.date_transaction.apply(lambda x: pd.to_datetime(x['$date'],errors='coerce'))
    
    #affectation du semestre de la transaction
    df['semester'] = df.date_transaction.dt.year.astype(str) + 'S' + np.where(df.date_transaction.dt.quarter.gt(2),2,1).astype(str)
    
    #selection des colonnes d'intéret dans le dataframe
    df=df[col]

    return df


#extraction des valeurs unique de la BD sur la colonne v

def extract_distinct_value(condition:dict,v:str):

    return collection.find(condition).distinct(v)


#extraction de toute la BD (sans requête) dans un dataframe - /!\trop long/!\  
def extract_from_DB_to_df(col):

    df = pd.DataFrame.from_dict(json.loads(dumps(collection.find())))
    df.set_index(['id_transaction'])
    df["date_transaction"] = df.date_transaction.apply(lambda x: pd.to_datetime(x['$date']).date())
    df['semester'] = df.date_transaction.dt.year.astype(str) + 'S' + np.where(df.date_transaction.dt.quarter.gt(2), 2,
                                                                              1).astype(str)

    df=df[col]
    return df




#A faire
def generate_tdb_quartier():
    return None

    #return collection.find(condition)

#ajouter une colonne dans la BD prix/moyen par m²
#Créer nouvelle table
#_id
#quartier
#year
#prix moyen/m²
#prix median/m²
#ecart_type m²
#Volume transac
#

'''class Transaction(BaseModel):

    #ID : StringField(default=str(ObjectIdField()))
    #adresse:str
    #code_postal:int
    #date_transaction:date
    #DCOMIRIS:str
    #departement:str
    #prix:Optional[float]=None
    #surface_habitable:int
    #DEPCOM:str
    #geometry:Point
    #id_transaction:str
    #id_ville:int
    #IRIS:str
    #latitude:str
    #longitude:str
    #n_pieces:Optional[int]=None
    #NOM_COM:str
    #NOM_IRIS:Optional[str]=None
    #TYPE_IRIS:Optional[str]=None
    #type_batiment:Optional[str]=None
    #vefa:Optional[bool]
    #ville:str'''





