import pandas as pd
from sklearn.preprocessing import MinMaxScaler

# Retourne la liste des départements contenu dans transactions
def get_departements(transactions):
    departements = list(transactions["departement"].unique())
    return departements

# Retourne la liste des villes d'un département donné contenu dans transactions
def get_villes(transactions, departement):
    villes = list(transactions[transactions["departement"]==departement]["villes"].unique())
    return villes

# Retourne la liste des quartiers d'une ville donnée contenu dans transactions
def get_quartiers(transactions, ville):
    quartiers = list(transactions[transactions["villes"]==ville]["NOM_IRIS"].unique())
    return quartiers

# Renvoie une base de données ciblée en fonction des données de géolocalisation
def data_selection(transactions, departement, ville, quartier):
    transactions = transactions[(transactions["departement"]==departement) & (transactions["ville"]==ville) & (transactions["NOM_IRIS"] == quartier)]
    return transactions

# Renvoie une base de données transformée à partir du dataset de la fonction data_selection
def data_transformation(df):
    
    # Feature Selection
    df = df[["prix", "vefa", "n_pieces", "surface_habitable"]]
    
    # One Hot Encoding
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
    
    # Retourne le dataset transformé ainsi que les valeurs minimales et maximales pour renvoyer la bonne prédiction future
    return df_scaled, prix_min, prix_max, n_pieces_min, n_pieces_max, surface_habitable_min, surface_habitable_max