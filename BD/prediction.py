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
    df_scaled, scaler = data_transformation(df)

    # Transformation des inputs de l'utilisateur
    
    #si l'utilisateur ne renseigne pas de nombre de pièces on prendra le nombre de pièce moyen du sous dataset
    if (n_pieces is None):
        n_pieces_norm= df.n_pieces.mean()
    else:
        n_pieces_norm = scaler.transform([[0,0,n_pieces, 0]])[0,2]
    
    surface_habitable_norm = scaler.transform([[0,0,0, surface_habitable]])[0,3]

    # Séparation target et features et split entrainement test
    X = df_scaled.drop(["prix","vefa"], axis=1)
    y = df_scaled["prix"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=123)

    # Récupération du modèle
    model, params = best_estimator(X_train, y_train)
    model.fit(X_train, y_train)

    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    mae_train = median_absolute_error(y_train * df["prix"].std() + df["prix"].mean(), y_pred_train * df["prix"].std() + df["prix"].mean())
    mae_test = median_absolute_error(y_test * df["prix"].std() + df["prix"].mean(), y_pred_test * df["prix"].std() + df["prix"].mean())

    prediction_norm = model.predict([[n_pieces_norm, surface_habitable_norm]])
    prediction = prediction_norm * df["prix"].std() + df["prix"].mean()

    return prediction, mae_train, mae_test, str(model), str(params),df.shape[0]
