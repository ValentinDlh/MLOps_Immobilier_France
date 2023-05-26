import pandas as pd
from sklearn.preprocessing import StandardScaler
import db as db
import numpy as np
from scipy import stats


# Retourne la liste des départements contenu dans transactions
def get_departements():
    dpt=db.extract_distinct_value({},"departement")
    return dpt


# Retourne la liste des villes d'un département donné contenu dans transactions
def get_villes(departement):
    villes = db.extract_distinct_value({'departement':str(departement)},"NOM_COM")
    return villes


# Retourne la liste des quartiers d'une ville donnée contenu dans transactions
def get_quartiers(ville):
    quartiers = db.extract_distinct_value({"NOM_COM":ville},"NOM_IRIS")
    return quartiers


# Renvoie une base de données ciblée en fonction des données de géolocalisation
def data_selection(departement,ville,quartier,col,sous_dataset:bool,param_sous_data:dict):
    #On definit query un dictionnaire qui définit les critères géographiques de transactions : ville et quartiers
    query={"departement":departement,"NOM_COM":ville,"NOM_IRIS":{"$in": quartier}}
    transactions=db.extract_from_DB_to_df_with_condition(query,col)

    #fonction generer_sous_dataset va prendre en compte les critères autres que geographique, à savoir : surface, n_pièces, vefa
    if sous_dataset:
        sous_data=generer_sous_dataset(df=transactions,col=col,surface=param_sous_data["surface_habitable"],n_pieces=param_sous_data["n_pieces"],vefa=param_sous_data["vefa"])
        return sous_data
    else:
        return transactions


# Renvoie une base de données transformée à partir du dataset de la fonction data_selection
def data_transformation(df):
    # Feature Selection
    df = df[["prix", "vefa", "n_pieces", "surface_habitable"]]

    # One Hot Encoding
    df["vefa"] = df["vefa"].astype(int)

    # StandardScaler
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(df)
    df_scaled = pd.DataFrame(scaled_data)
    df_scaled = df_scaled.rename(columns=dict(zip(df_scaled.columns, df.columns)))

    # Retourne le dataset transformé ainsi que le scaler
    return df_scaled, scaler


def generer_sous_dataset(df,surface,n_pieces,vefa,col,lim_nb_ligne=25,percent_ecart=0.25):
    #lim_nb_ligne est le nombre de ligne maximal du sous dataset final qui sera utiliser pour faire une prediction
    
    #percent_ecart est le pourcentage d'écart en surface que l'on va utiliser pour filtrer les transactions d'intéret - ex un appartement de 30m² +ou- 25%     
    ecart_surface = surface*percent_ecart
    
    #Pour chaque critère on calcul l'écart entre le critère du demandeur et la variable correspondante de chaque transaction du dataset
    df['sort_surf'] = df.surface_habitable.apply(lambda x: None if surface is None else abs(x - surface))
    df['sort_nb_p'] = df.n_pieces.apply(lambda x: None if n_pieces is None else abs(x - n_pieces))
    df['sort_vefa'] = df.vefa.apply(lambda x: None if vefa is None else x & vefa)
    
    #On filtre les transactions similaires selon la surface habitable
    df = df[df['sort_surf'] <= ecart_surface]
    
    #on calcul le prix au m2
    df['prix_m2'] = df.apply(lambda x: x['prix'] / x['surface_habitable'], axis=1)
    
    #on supprime les outliers avec le zscore
    df=df[(np.abs(stats.zscore(df['prix_m2'])) < 2.0)]  
    
    prix_med_m2=df['prix_m2'].median()
    df['sort_prix']=df.prix_m2.apply(lambda x: abs(x-prix_med_m2))
    
    #On trie le dataset selon les transactions les plus proches des critères fournies par le demandeurs et selon les prix les plus proches de la mediane
    df=df.sort_values(by=['semester','sort_vefa','sort_prix','sort_nb_p'], ascending=(False,False,True,True)).head(lim_nb_ligne)
    df = df[col]
    return df