from sklearn.model_selection import GridSearchCV
from sklearn.linear_model import LinearRegression, ElasticNet


def optimize_linear_regression(X, y):
    """
    Optimisation des hyperparamètres d'un modèle de régression linéaire simple.
    """

    model = LinearRegression()

    parameters = {'fit_intercept': [True, False],
                  'copy_X': [True, False]}

    # Optimisation des hyperparamètres basé sur la rmse
    grid_search = GridSearchCV(model, parameters, cv=5, scoring='neg_root_mean_squared_error')
    grid_search.fit(X, y)

    return grid_search.best_estimator_, grid_search.best_params_, -1*grid_search.best_score_


def optimize_elastic_net(X, y):
    """
    Optimisation des hyperparamètres d'un modèle de régression linéaire ElasticNet.
    """
    param_grid = {'alpha': [0.1, 1.0, 10.0],
                  'l1_ratio': [0.25, 0.5, 0.75]}

    elastic_net = ElasticNet()

    # Optimisation des hyperparamètres basé sur la rmse
    grid_search = GridSearchCV(elastic_net, param_grid, cv=5, scoring='neg_root_mean_squared_error')
    grid_search.fit(X, y)

    return grid_search.best_estimator_, grid_search.best_params_, -1*grid_search.best_score_


def best_estimator(X, y):
    """
    On retourne le meilleur modèle.
    """
    model_LR, params_LR, score_LR = optimize_linear_regression(X, y)
    model_EN, params_EN, score_EN = optimize_elastic_net(X, y)

    # On retourne le modèle qui donne une rmse la plus faible
    if score_LR < score_EN:
        return model_LR, params_LR
    else:
        return model_EN, params_EN