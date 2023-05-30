# Immobilier France Prediction API
Ce projet fournit une API pour prédire le prix d'un appartement en france en fonction des données géographiques, de la surface habitable et du nombre de pièces. L'API est construite à l'aide de FastAPI et la base de données est constitué de 1.4M transactions immobilières stockées sur MongoDB Atlas. Le serveur est exécuté avec Uvicorn.


## Data Source
L'ensemble de données utilisé a été obtenu à partir du Kaggle suivant :
https://www.kaggle.com/datasets/benoitfavier/immobilier-france

Ces données sont open source, publiées et mises à jour par le gouvernement français. La méthodologie de récupération de ces données est spécifié sur le github suivant :
https://github.com/BFavier/DVFplus

## Install & Run

AVEC DOCKER

1) Aller dans le dossier docker-compose
2) Run docker-compose up
3) Aller à l'URL http://127.0.0.1:8000/docs/ pour pouvoir voir et tester l'API.
