import pandas as pd
from sklearn.preprocessing import StandardScaler
import db as db
import numpy as np
from scipy import stats
from json import dumps


# Retourne la liste des départements contenu dans transactions
def get_departements():
    dpt = db.extract_distinct_value({}, "departement")
    return dpt


# Retourne la liste des villes d'un département donné contenu dans transactions
def get_villes(departement):
    villes = db.extract_distinct_value({'departement': departement}, "NOM_COM")
    return villes


# Retourne la liste des quartiers d'une ville donnée contenu dans transactions
def get_quartiers(ville):
    quartiers = db.extract_distinct_value({"NOM_COM": ville}, "NOM_IRIS")
    return quartiers

def get_tendance_ville(departement,ville,max_year):
    #on récupère le Tdb_Quartier dans dataframe
    data_brut = pd.DataFrame.from_dict(db.get_data_from_db({}, 'Tdb_Quartier'))

    min_year=data_brut.ANNEE.min()


    #calcul de l'évolution des moyennes national en nb de transactions et prix_moyen_m2

    evolution_national_nb_transaction=round(
        (
                (data_brut[data_brut['ANNEE']==max_year]['COUNT_id_transaction'].sum()-data_brut[data_brut['ANNEE']==min_year]['COUNT_id_transaction'].sum())
                /data_brut[data_brut['ANNEE']==min_year]['COUNT_id_transaction'].sum()),
        3)*100

    nb_transac_year_min=data_brut[data_brut['ANNEE']==min_year].COUNT_id_transaction.sum()
    nb_transac_year_max = data_brut[data_brut['ANNEE'] == max_year].COUNT_id_transaction.sum()

    data_brut['prix_m2_ponderé']=data_brut.apply(lambda x: int(x['Prix_moyen_m2']*x['COUNT_id_transaction']),axis=1)

    prix_m2_national_moyen_year_min=data_brut[data_brut['ANNEE']==min_year]['prix_m2_ponderé'].sum()/nb_transac_year_min
    prix_m2_national_moyen_year_max = data_brut[data_brut['ANNEE'] == max_year]['prix_m2_ponderé'].sum() / nb_transac_year_max

    evolution_national_prix_m2=round(
        ((prix_m2_national_moyen_year_max-prix_m2_national_moyen_year_min)
         /prix_m2_national_moyen_year_min),3)*100


    # calcul de l'évolution des moyennes sur la ville en nb de transactions et prix_moyen_m2

    data_brut_ville = data_brut[(data_brut['NOM_COM'] == ville) & (data_brut['departement'] == departement)]
    if (data_brut_ville.empty):
        return None

    evolution_ville_nb_transaction = (
                (data_brut_ville[data_brut_ville['ANNEE'] == max_year]['COUNT_id_transaction'].sum() -
                 data_brut_ville[data_brut_ville['ANNEE'] == min_year]['COUNT_id_transaction'].sum())
                / data_brut_ville[data_brut_ville['ANNEE'] == min_year]['COUNT_id_transaction'].sum()) * 100

    evolution_ville_prix_m2=(
                (data_brut_ville[data_brut_ville['ANNEE'] == max_year]['Prix_moyen_m2'].sum() -
                 data_brut_ville[data_brut_ville['ANNEE'] == min_year]['Prix_moyen_m2'].sum())
                / data_brut_ville[data_brut_ville['ANNEE'] == min_year]['Prix_moyen_m2'].sum()) * 100

    d={}
    d['Ville']=ville
    d['annee_depart']=int(min_year)
    d['annee_fin'] = int(max_year)
    #print(data_brut_ville[data_brut_ville['ANNEE'] == min_year]['COUNT_id_transaction'].iloc[0])
    d['nb_transactions_'+str(min_year)]=int(data_brut_ville[data_brut_ville['ANNEE'] == min_year]['COUNT_id_transaction'].iloc[0])
    d['nb_transactions_' + str(max_year)] =int(data_brut_ville[data_brut_ville['ANNEE'] == max_year]['COUNT_id_transaction'].iloc[0])

    d['evolution_ville_nb_transactions'] = dumps(round(evolution_ville_nb_transaction,2))
    d['evolution_ville_prix_m2'] = dumps(round(evolution_ville_prix_m2,2))

    d['evolution_nb_transactions_moyenne_nationnale'] = dumps(round(evolution_national_nb_transaction,2))
    d['evolution_prix_m2_moyenne_nationnale'] = dumps(round(evolution_national_prix_m2,2))

    return d

# Renvoie une base de données ciblée en fonction des données de géolocalisation
def data_selection(departement, ville, quartier, col, sous_dataset: bool, param_sous_data: dict):
    # On definit query un dictionnaire qui définit les critères géographiques de transactions : ville et quartiers

    query = {"departement": int(departement), "NOM_COM": ville, "NOM_IRIS": {"$in": quartier}}

    transactions = db.extract_from_DB_to_df_with_condition(query, col)
    if transactions==None :
        return None

    # fonction generer_sous_dataset va prendre en compte les critères autres que geographique, à savoir : surface, n_pièces, vefa
    if sous_dataset:
        sous_data = generer_sous_dataset(df=transactions, col=col, surface=param_sous_data["surface_habitable"],
                                         n_pieces=param_sous_data["n_pieces"], vefa=param_sous_data["vefa"])
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


def generer_sous_dataset(df, surface, n_pieces, vefa, col, zscore=2.0, percent_ecart=0.25):
    # lim_nb_ligne est le nombre de ligne maximal du sous dataset final qui sera utiliser pour faire une prediction
    if df.empty:
        return None
    # percent_ecart est le pourcentage d'écart en surface que l'on va utiliser pour filtrer les transactions d'intéret - ex un appartement de 30m² +ou- 25%
    ecart_surface = surface * percent_ecart

    # Pour chaque critère on calcul l'écart entre le critère du demandeur et la variable correspondante de chaque transaction du dataset
    df['sort_surf'] = df.surface_habitable.apply(lambda x: None if surface is None else abs(x - surface))
    #df['sort_nb_p'] = df.n_pieces.apply(lambda x: None if n_pieces is None else abs(x - n_pieces))
    #df['sort_vefa'] = df.vefa.apply(lambda x: None if vefa is None else x & vefa)

    # On filtre les transactions similaires selon la surface habitable
    df = df[df['sort_surf'] <= ecart_surface]

    # on supprime les outliers avec le zscore
    df = df[(np.abs(stats.zscore(df['prix_m2'])) < zscore)]

    #si l'utilisateur a renseigné une valeur pour vefa ou un nombre de pièces, on filtre le sous dataset
    if vefa!=None :
        df=df[df['vefa']==vefa]
    if n_pieces !=None:
        df = df[df['n_pieces'] == n_pieces]


    # On trie le dataset selon les transactions les plus proches des critères fournies par le demandeurs et selon les prix les plus proches de la mediane
    df = df[col]
    return df
