from sklearn.model_selection import GridSearchCV
from sklearn.linear_model import LinearRegression, ElasticNet
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score


def compute_model_score(model, X, y):
    """
    Retourne le score moyen du modèle avec une cross validation.
    """

    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()

    return model_score


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


def optimize_decision_tree(X, y):
    """
    Optimisation des hyperparamètres d'un modèle de régression DecisionTreeRegressor.
    """
    param_grid = {'max_depth': [5, 10, 15],
                  'min_samples_split': [2, 5, 10]}

    dt = DecisionTreeRegressor()

    # Optimisation des hyperparamètres basé sur la rmse
    grid_search = GridSearchCV(dt, param_grid, cv=5, scoring='neg_root_mean_squared_error')
    grid_search.fit(X, y)

    return grid_search.best_estimator_, grid_search.best_params_, -1*grid_search.best_score_


def optimize_random_forest(X, y):
    """
    Optimisation des hyperparamètres d'un modèle de régression RandomForestRegressor.
    """
    param_grid = {'n_estimators': [100, 200, 300],
                  'max_depth': [5, 10, 15],
                  'min_samples_split': [2, 5, 10]}

    rf = RandomForestRegressor()

    # Optimisation des hyperparamètres basé sur la rmse
    grid_search = GridSearchCV(rf, param_grid, cv=5, scoring='neg_root_mean_squared_error')
    grid_search.fit(X, y)

    return grid_search.best_estimator_, grid_search.best_params_, -1*grid_search.best_score_


def best_estimator(X, y):
    """
    On retourne le meilleur modèle.
    """
    score_lr = -compute_model_score(LinearRegression(), X, y)
    score_en = -compute_model_score(ElasticNet(), X, y)
    score_dt = -compute_model_score(DecisionTreeRegressor(), X, y)
    score_rf = -compute_model_score(RandomForestRegressor(), X, y)

    if (score_lr < score_dt) & (score_lr < score_rf) & (score_lr < score_en):
        model_LR, params_LR, score_LR = optimize_linear_regression(X, y)
        return model_LR, params_LR, score_LR
    elif (score_en < score_lr) & (score_en < score_dt) & (score_en < score_rf):
        model_EN, params_EN, score_EN = optimize_elastic_net(X, y)
        return model_EN, params_EN, score_EN
    elif (score_dt < score_lr) & (score_dt < score_en) & (score_dt < score_rf):
        model_DT, params_DT, score_DT = optimize_decision_tree(X, y)
        return model_DT, params_DT, score_DT
    else:
        model_RF, params_RF, score_RF = optimize_random_forest(X, y)
        return model_RF, params_RF, score_RF