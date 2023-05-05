FROM debian:latest

ADD requirements.txt api_projet.py db.py modelisation.py prediction.py preprocessing.py ./

RUN apt update && apt install python3-pip -y && pip install -r requirements.txt

EXPOSE 8000

CMD uvicorn api_projet:api --host 0.0.0.0