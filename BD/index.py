from mongoengine import connect
from models.transactions import immo
from fastapi import FastAPI
import json

api=FastAPI()
connect(db="Transactions", host="localhost",port=27017)

@api.get("/postcode")
def get_by_postcode(COM: str):
    print(immo.objects)
    #transac=json.loads(Immo.objects.filter(NOM_COM=COM).to_json())

    return{"transaction":""}# transac}

#Immo.objects().filter(adresse__icontains=adresse)
# permet de recupérer toutes les valeurs qui contienne l'adresse : rue victor hugo, RUE Victor Hugo,...
