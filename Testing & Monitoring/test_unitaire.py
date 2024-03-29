import datetime as dt
import db as db
import json
import pandas as pd




def get_metrics(f):
    with open(f, 'r') as file:
        res = json.load(file)

    return res


def write_metric(dic, f):
    with open(f, 'w') as json_file:
        json.dump(dic, json_file)




def test_nb_ligne_BD(current_nb_row, collection):
    res = db.db[collection].count_documents({})

    if (res > current_nb_row) | ((res == current_nb_row) & (collection == 'Tdb_Quartier')):
        print('test OK : import de ' + str(res - current_nb_row) + ' ligne(s) dans la table ' + collection)
        return True, res
    else:
        print('test KO : import de ' + str(res - current_nb_row) + ' ligne dans la table ' + collection)
        return False, 0


def test_rqt(df, collection):

    list_ville = df.Ville.to_list()
    y = int(df.ANNEE.unique()[0])

    l = []
    # calcul des resultats des requêtes avec les nouvelles données
    test = True
    st = ''
    for v in list_ville:
        rqt = [
            {
                '$group': {
                    '_id': {
                        'departement': '$departement',
                        'NOM_COM': '$NOM_COM',
                        'ANNEE': {'$year': '$date_transaction'}
                    },
                    'Prix_moyen': {'$avg': '$prix'},
                    'COUNT_id_transaction': {'$sum': 1}
                }
            },
            {
                '$match': {
                    '_id.NOM_COM': v,
                    '_id.ANNEE': y
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "Ville": "$_id.NOM_COM",
                    "ANNEE": "$_id.ANNEE",
                    "Prix_moyen": {'$toInt': '$Prix_moyen'},  # Conversion en entier
                    'COUNT_id_transaction': 1
                }
            }
        ]

        res = db.rqt_bd(rqt, collection)[0]  # db.db[collection].aggregate(rqt)[0]
        dic_temp = {}
        dic_temp['Ville'] = v
        dic_temp['ANNEE'] = y
        dic_temp['COUNT_id_transaction'] = res['COUNT_id_transaction']
        dic_temp['Prix_moyen'] = res['Prix_moyen']
        l.append(dic_temp)

        if (res['COUNT_id_transaction'] == df[(df['Ville'] == v)].COUNT_id_transaction.iloc[0]):
            st = st + 'Ville ' + v + ' - count OK : ' + str(res['COUNT_id_transaction']) + ' \n'
        else:
            st = st + 'Ville ' + v + ' - count KO: avant import =' + str(
                df[(df['Ville'] == v)].COUNT_id_transaction.iloc[0]) + ' VS aprés import = ' + str(
                res['COUNT_id_transaction']) + '\n'
            test = False
        if (abs(df[(df['Ville'] == v)].Prix_moyen.iloc[0] - res['Prix_moyen']) < 5):
            st = st + 'Ville ' + v + ' - prix_moyen OK : ' + str(res['Prix_moyen']) + ' \n'
        else:
            st = st + 'Ville ' + v + ' - prix_moyen KO: avant import = ' + str(
                df[(df['Ville'] == v)].Prix_moyen.iloc[0]) + ' VS aprés import = ' + str(res['Prix_moyen']) + '\n'
            test = False


    if test == True:
        return test, l
    else:
        l = []
        return test, l


