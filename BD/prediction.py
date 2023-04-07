from sklearn.model_selection import train_test_split
from sklearn.metrics import median_absolute_error
from preprocessing import data_selection, data_transformation
from modelisation import best_estimator



def prediction(departement, ville, quartier,vefa, n_pieces, surface_habitable,col):
    """
    Renvoie la prédiction de notre modèle basé sur les inputs de l'utilisateur.
    """
    # Détermination du dataset transactions transformé et récupération des valeurs max et min des variables
    
    #dictionnaire p contient les critères de filtre qui seront transmis dans une requête pour interroger la BD
    p={}
    p["vefa"]=vefa
    p["n_pieces"]=n_pieces
    p["surface_habitable"]=surface_habitable
    
    
    #séléction des données pertinentes en fonction des critères du dictionnaire p
    df = data_selection(departement, ville,quartier,col,sous_dataset=True,param_sous_data= p)


    #data_selection(departement, ville, quartier, col)
    df_scaled, prix_min, prix_max, n_pieces_min, n_pieces_max, surface_habitable_min, surface_habitable_max = data_transformation(
        df)

    # Transformation des inputs de l'utilisateur
    
    #si l'utilisateur ne renseigne pas de nombre de pièces on prendra le nombre de pièce moyen du sous dataset
    if (n_pieces is None):
        n_pieces_norm= df.n_pieces.mean()
    else:
        n_pieces_norm = (n_pieces - n_pieces_min) / (n_pieces_max - n_pieces_min)
    surface_habitable_norm = (surface_habitable - surface_habitable_min) / (
                surface_habitable_max - surface_habitable_min)



    # Séparation target et features et split entrainement test
    X = df_scaled.drop(["prix","vefa"], axis=1)
    y = df_scaled["prix"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=123)

    # Récupération du modèle
    model, params = best_estimator(X_train, y_train)
    model.fit(X_train, y_train)

    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    mae_train = median_absolute_error(y_train * (prix_max - prix_min) + prix_min,
                                      y_pred_train * (prix_max - prix_min) + prix_min)
    mae_test = median_absolute_error(y_test * (prix_max - prix_min) + prix_min,
                                     y_pred_test * (prix_max - prix_min) + prix_min)

    prediction_norm = model.predict([[n_pieces_norm, surface_habitable_norm]])
    prediction = prediction_norm * (prix_max - prix_min) + prix_min

    return prediction, mae_train, mae_test, str(model), str(params),df.shape[0]
