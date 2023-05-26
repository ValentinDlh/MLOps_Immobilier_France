import pandas as pd
import numpy as np
import datetime
from db import collection

import geopandas as gpd
from shapely.geometry import Point

import subprocess
from zipfile import ZipFile 

  
# spécifiant le nom du fichier zip
file = "transactions.npz.zip"
# Command Kaggle pour import new bdd
command = "kaggle datasets download benoitfavier/immobilier-france -f transactions.npz"

def import_new_dataset():

    # Exécutez la commande dans le terminal
    subprocess.run(command, shell=True)

def file_extract():
    
    # ouvrir le fichier zip en mode lecture
    with ZipFile(file, 'r') as zip: 
        zip.extractall() 

def npz_to_df():
    ''' 
    Conversion du fichier transactions.npz en dataframe
    '''
    arrays = dict(np.load("transactions.npz"))
    data = {k: [s.decode("utf-8") for s in v.tobytes().split(b"\x00")] if v.dtype == np.uint8 else v for k, v in arrays.items()}
    transactions = pd.DataFrame.from_dict(data)
    return transactions

def get_date_max_new_transactions(transactions):
    ''' 
    Retourne la date la plus récente de la BDD récente
    '''
    most_recent_date = max(transactions["date_transaction"])

    # Extraire la valeur du timestamp en secondes
    timestamp_seconds = most_recent_date.timestamp()

    # Conversion du timestamp en objet datetime
    most_recent_date_new = datetime.datetime.fromtimestamp(timestamp_seconds)

    return most_recent_date_new


def get_date_max_old_transactions(collection):
    ''' 
    Retourne la date la plus récente de la BDD ancienne
    '''
    # Utiliser l'agrégation pour obtenir la date maximale
    pipeline = [
        {"$group": {"_id": None, "maxDate": {"$max": "$date_transaction"}}}
    ]

    result = list(collection.aggregate(pipeline))

    # Extraire la date maximale du résultat
    most_recent_date_old = result[0]["maxDate"]

    return most_recent_date_old 


def batch_iris(df):
    
    geoiris = gpd.read_file("iris-2013-01-01/iris-2013-01-01.shp")

    crs = {'init': 'epsg:4326'}
    geometry = [Point(xy) for xy in zip(df.longitude, df.latitude)]
    geo_transaction = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

    return gpd.sjoin(geo_transaction, geoiris)


def generate_new_transactions(new_df):
    ''' 
    Preprocessing et génération du dataframe final à intégrer dans mongodb
    '''
    
    new_df.drop(["index_right"], axis=1)
    new_df = new_df.drop(["index_right"], axis=1)
    new_df.drop_duplicates(inplace=True)
    
    new_df = new_df[new_df["surface_habitable"] > 0]
    new_df["prix_m2"] = new_df["prix"]/new_df["surface_habitable"]
    new_df = new_df[["id_transaction", "date_transaction", "prix", "prix_m2", "adresse", "NOM_COM", "code_postal", "departement", "NOM_IRIS", "TYP_IRIS", "vefa", "n_pieces", "surface_habitable"]]
    
    return new_df


def import_new_transactions(new_df):

    # DF to dict
    data = new_df.to_dict(orient='records')

    # insertion des nouvelles transactions dans mongodb
    collection.insert_many(data)


if __name__ == '__main__':

    import_new_dataset()
    print("import zip done !")
    file_extract()
    print("extract npz done !")
    df = npz_to_df()

    most_recent_date_new = get_date_max_new_transactions(df)
    print(most_recent_date_new)
    most_recent_date_old = get_date_max_old_transactions(collection)
    print(most_recent_date_old)

    if most_recent_date_new > most_recent_date_old:
        new_transactions = df[df["date_transaction"] > most_recent_date_old]
        print(new_transactions.columns)
        new_data = batch_iris(new_transactions)
        print(new_data.columns)
        new_data_final = generate_new_transactions(new_data)
        print(new_data_final.columns)
        import_new_transactions(new_data_final)
        print("done !")