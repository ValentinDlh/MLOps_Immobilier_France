from db import collection
import pymongo
#static method nb ligne

nb_row=1426244
req=[{}]
req_res=[{}]


def test_nb_ligne_BD(con):
    return isinstance(con.count_documents({}), int)


def test_delta_nb_ligne_BD(con,nb_row):

    return con.count_documents({})-nb_row>0

def test_requete(con, req,req_res):

    return None