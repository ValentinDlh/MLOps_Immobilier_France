from fastapi import FastAPI, HTTPException, status, Depends,Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from passlib.context import CryptContext
from typing import List,Optional
import prediction as pred
import preprocessing
import datetime

# Importation des données

#définition de la liste des colonnes du dataset que l'on souhaite afficher à l'utilisateur
col=['prix','prix_m2','id_transaction', 'surface_habitable', 'n_pieces', 'date_transaction', 'semester',
       'vefa', 'adresse', 'code_postal', 'NOM_COM', 'NOM_IRIS', 'TYP_IRIS']

# Description des endpoints de l'API
description_transactions = f"il faut renseigner une date au format aaaa-mm-jj \
            , une période en mois, le numéro du département et le nom de la ville en majuscule (voir la section GET VILLES)."

api = FastAPI(openapi_tags=[{"name":"Géolocalisation",
                             "description":"Obtenir les numéros de départements et les villes associées"},
                            {"name": "Transactions",
                             "description": "Génération de transactions immobilières"},
                            {"name": "Prédictions",
                             "description": "Génération d'une prédiction du prix d'un appartement"}],
                title='Immobilier France API',
                description="Obtenir des informations sur les transactions immobilières en France et des prédictions sur le prix d'un appartement",
                version="1.0.1")

# Authentification HTTP
security = HTTPBasic()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

users = {

    "valentin": {
        "username": "valentin",
        "name": "Valentin Delahaye",
        "hashed_password": pwd_context.hash('valentindlh'),
    },

    "ugo" : {
        "username" :  "ugo",
        "name" : "Ugo-Jean Loiodice",
        "hashed_password" : pwd_context.hash('ugojean'),
    }

}

# Gestion des erreurs
responses = {
    200: {"description": "OK"},
    401: {"description": "Problème d'authentification"}
}


# Formulaire d'authentification et vérification d'identifiants
def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    username = credentials.username
    password = credentials.password
    if not(users.get(username)) or not(pwd_context.verify(password, users[username]["hashed_password"])):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return username

# Endpoints
@api.get("/user", name = "Utilisateur actuel", tags =["Current User"])
def current_user(username: str = Depends(get_current_user)):
    return "Hello {}".format(username)

@api.get('/', include_in_schema=False)
def get_index():
    return "L'API est fonctionnelle"

@api.get('/departements', tags=["Géolocalisation"], description="Obtenir les départements", responses=responses)
def get_departements():
    departements = {}
    departements["departements"] = sorted(preprocessing.get_departements())
    return departements

@api.get('/departements/villes', tags=["Géolocalisation"], description="Retourne les villes du département renseigné (voir la liste des numéros de départements via la requête get_departements)", responses=responses)
def get_villes(departement: int):
    villes = {}
    villes["villes"] = sorted(preprocessing.get_villes(departement))
    return villes

@api.get('/departements/quartiers', tags=["Géolocalisation"], description="Retourne les quartiers de la ville renseignée (voir la liste des villes via la requête get_villes)", responses=responses)
def get_quartiers(ville: str):
    quartiers = {}
    quartiers["quartiers"] = sorted(preprocessing.get_quartiers(ville))
    return quartiers

@api.get('/transactions', tags=["Transactions"], description="Retourne les transactions spécifiques à une période et à une ville: "+description_transactions, responses=responses)
def get_transactions(departement : str, ville : str, quartier : List[str] = Query(None)):
    p={}
    error={}
    ville=ville.upper()
    quartier=[s.upper() for s in quartier]
    df = preprocessing.data_selection(departement, ville, quartier, col, sous_dataset=False, param_sous_data=p)
    #df = df[["date_transaction", "prix", "departement", "adresse", "ville", "code_postal", "vefa", "n_pieces", "surface_habitable", "NOM_IRIS"]]
    if df==None:
        error['error']={'No data with parameters sent'}
        return error
    return df.to_dict(orient='records')

@api.get('/predictions', tags=["Prédictions"], description="Retourne la prédiction du prix d'un appartement avec un intervalle de confiance", responses=responses)
def get_prediction(departement: str, ville : str,  surface_habitable : int,vefa : Optional[bool]=None, n_pieces : Optional[int]=None, quartier : List[str]=Query(None)):
    p = {}
    error = {}
    ville=ville.upper()
    quartier=[s.upper() for s in quartier]

    if pred.prediction(departement, ville, quartier, vefa, n_pieces, surface_habitable, col)==None :
        error['error']={'No data with parameters sent'}
        return error

    prediction_prix, mae_train, mae_test, model, params, nb, score = pred.prediction(departement, ville, quartier, vefa, n_pieces, surface_habitable, col)
    p["prediction_prix"] = int(prediction_prix[0])
    p["intervalle_confiance_montant"] = int(mae_test)
    p["intervalle_confiance_ratio"] = round(mae_test/prediction_prix[0],2)
    p["mae_train"] = int(mae_train)
    p["echantillon"] = nb
    p["modèle"] = model
    p["paramètre"] = params
    p["rmse"] = round(score, 2)

    return p


@api.get('/Tendance', tags=["Prédictions"], description="Retourne la tendance du prix au m² par an", responses=responses)
def get_tendance(departement: int, ville : str, annee_fin : Optional[int]=None):
    res={}
    error={}
    if annee_fin==None:
        annee_fin=(datetime.datetime.now().year)-1

    if((annee_fin<2019) | (annee_fin>2022)):
        error['error']={'année_fin doit être compris entre 2019 et 2022'}
        return error
    else:
        res=preprocessing.get_tendance_ville(departement,ville,annee_fin)
        if res==None :
            error['error']={'No data with parameters sent'}
            return error

    return res
