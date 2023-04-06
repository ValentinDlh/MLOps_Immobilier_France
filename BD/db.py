#from pydantic import BaseModel
import pandas as pd
#from typing import Optional
import datetime
import pymongo
import json
from bson.json_util import dumps
import numpy as np



mongoURI="mongodb://localhost:27017"
client=pymongo.MongoClient(mongoURI)
db=client['Transactions']
collection=db['Immo']



def get_one(c:str):
    return collection.find_one({"_id": c})

def extract_from_DB_to_df_with_condition(condition:dict,col):
    #df=pd.DataFrame(list(collection.find()))

    df = pd.DataFrame.from_dict(json.loads(dumps(collection.find(condition))))
    df.set_index(['id_transaction'])
    df["date_transaction"] = df.date_transaction.apply(lambda x: pd.to_datetime(x['$date'],errors='coerce'))
    df['semester'] = df.date_transaction.dt.year.astype(str) + 'S' + np.where(df.date_transaction.dt.quarter.gt(2),2,1).astype(str)
    df=df[col]

    return df


def extract_distinct_value(condition:dict,v:str):

    return collection.find(condition).distinct(v)

def extract_from_DB_to_df(col):

    df = pd.DataFrame.from_dict(json.loads(dumps(collection.find())))
    df.set_index(['id_transaction'])
    df["date_transaction"] = df.date_transaction.apply(lambda x: pd.to_datetime(x['$date']).date())
    df['semester'] = df.date_transaction.dt.year.astype(str) + 'S' + np.where(df.date_transaction.dt.quarter.gt(2), 2,
                                                                              1).astype(str)

    df=df[col]
    return df




def agg_from_DB_to_df(condition: dict, col):
    #l=[]
    #retourne liste dpt, list quartier pour un dpt
    #df = pd.DataFrame.from_dict(json.loads(dumps(collection.find(condition))))
    #df.set_index(['id_transaction'])
    #df = df[col]
    return None

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





