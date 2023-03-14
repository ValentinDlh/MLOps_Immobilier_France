import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

transactions = pd.read_csv("Data/transactions_2021_2022.csv")

def data_selection(date, periode, departement, ville):
    """
    Renvoie une base de données ciblée en fonction des données temporelles et de géolocalisation.
    """
    # Récupérer la date initiale pour délimiter la période de transactions
    transactions['date_transaction'] = pd.to_datetime(transactions['date_transaction'])
    date = pd.to_datetime(date)
    date_init = date - pd.DateOffset(months=periode)
    df = transactions[(transactions['date_transaction'] >= date_init) & (transactions['date_transaction'] <= date)]
    
    # Récupérer les transactions d'appartements de la ville et du département concerné
    df = df[(transactions["departement"]==departement) & (df["ville"]==ville) & (df["type_batiment"]=="Appartement")]
    
    return df


def data_transformation(df):
    """
    Renvoie une base de données transformée à partir du dataset de la fonction data_selection.
    """
    # Feature Selection
    df = df[["prix", "vefa", "n_pieces", "surface_habitable"]]
    
    # One Hot Encoding
    df = df.replace({"Appartement":0, "Maison":1})
    df["vefa"] = df["vefa"].astype(int)
    
    # Détermination des float min et max
    prix_min = df["prix"].min()
    prix_max = df["prix"].max()
    n_pieces_min = df["n_pieces"].min()
    n_pieces_max = df["n_pieces"].max()
    surface_habitable_min = df["surface_habitable"].min()
    surface_habitable_max = df["surface_habitable"].max()
    
    # MinMaxScaler
    scaler = MinMaxScaler()
    df_scaled = pd.DataFrame(scaler.fit_transform(df))
    df_scaled = df_scaled.rename(columns=dict(zip(df_scaled.columns, df.columns)))
    
    # Retourner le dataset transformé ainsi que les valeurs minimales et maximales pour renvoyer la bonne prédiction future
    return df_scaled, prix_min, prix_max, n_pieces_min, n_pieces_max, surface_habitable_min, surface_habitable_max