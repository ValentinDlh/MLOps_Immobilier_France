#from pydantic import BaseModel
import pandas as pd
#from typing import Optional
#from datetime import date
import pymongo
import json
from bson.json_util import dumps



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
    df["date_transaction"] = df.date_transaction.apply(lambda x: pd.to_datetime(x['$date']).date())
    df=df[col]

    return df


def extract_distinct_value_with_condition(condition:dict,v):
    #return list de valeur unique sur une requête avec condition
    return collection.find(condition).distinct(v)

def extract_from_DB_to_df(col):
    #df = pd.DataFrame(list(collection.find()))
    df = pd.DataFrame.from_dict(json.loads(dumps(collection.find())))
    df.set_index(['id_transaction'])
    df["date_transaction"] = df.date_transaction.apply(lambda x: str(pd.to_datetime(x['$date']).date()))
    df=df[col]
    return df


def generate_tdb_quartier():
    
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
    return None




'''class Transaction():
    def __init__(self,vefa:boolean,type_batiment:str,TYPE_IRIS:str,NOM_IRIS:str,NOM_COM:str,n_pieces:int,latitude:str,longitude:str,IRIS:str,prix:float,code_postal:int,date_transaction:datetime.date,DCOMIRIS:str,departement:str,adresse:str,surface_habitable:int,geometry:str,id_transaction:str,id_ville:int):

    #self._id=id
    #self.adresse=adresse
    #self.code_postal=code_postal
    #self.date_transaction=date_transaction
    #self.DCOMIRIS=DCOMIRIS
    #self.departement=departement
    #self.prix=prix
    #self.surface_habitable=surface_habitable
    #self.geometry=geometry
    #self.id_transaction=id_transaction
    #self.id_ville=id_ville
    #self.IRIS=IRIS
    #self.latitude=latitude
    #self.longitude=longitude
    #self.n_pieces:int
    #self.NOM_COM=NOM_COM
    #self.NOM_IRIS:str
    #self.TYPE_IRIS=TYPE_IRIS
    #self.type_batiment=type_batiment
    #self.vefa=vefa






