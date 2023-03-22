from fastapi import FastAPI, Query, HTTPException, status, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from typing import Optional, List
from passlib.context import CryptContext

import pandas as pd
import numpy as np
import random

from preprocessing import data_selection, data_transformation
from modelisation import best_estimator
from prediction import prediction

# Importation des données
transactions = pd.read_csv("Data/transactions_2021_2022.csv")
departements = sorted(list(transactions["departement"].unique()))

# Description des endpoints de l'API
description_villes = f"il faut renseigner le numéro du département parmi les choix suivants : {departements}"
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

@api.get('/departements/villes', tags=["Géolocalisation"], description="Obtenir les villes d'un département: "+description_villes, responses=responses)
def get_villes(departement: int):
    villes = {}
    villes["departement"] = sorted(list(transactions[transactions["departement"]==departement]["ville"].unique()))
    return villes

@api.get('/transactions', tags=["Transactions"], description="Retourne les transactions spécifiques à une période et à une ville: "+description_transactions, responses=responses)
def get_transactions(date : str, periode : int, departement : int, ville : str):
    df = data_selection(date, periode, departement, ville)
    return df.to_dict()

@api.get('/predictions', tags=["Prédictions"], description="Retourne la prédiction du prix d'un appartement avec un intervalle de confiance", responses=responses)
def get_prediction(date : str, periode : int, departement : int, ville : str, vefa : str, n_pieces : int, surface_habitable : int):
    predictions = {}
    prediction_prix, mae_train, mae_test, model, params = prediction(date, periode, departement, ville, vefa, n_pieces, surface_habitable)
    predictions["prediction_prix"] = prediction_prix[0]
    predictions["intervalle_confiance_montant"] = mae_test
    predictions["intervalle_confiance_ratio"] = mae_test/prediction_prix[0]
    predictions["mae_train"] = mae_train
    predictions["modèle"] = model
    predictions["paramètre"] = params
    return predictions