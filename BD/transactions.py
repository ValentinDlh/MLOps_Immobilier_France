from mongoengine import Document,ObjectIdField,StringField, IntField,FloatField,BooleanField,DateTimeField,GeoPointField


class Immo(Document):
    _id: ObjectIdField(primary_key=True)
    adresse:StringField()
    code_postal:IntField()
    date_transaction:DateTimeField()
    DCOMIRIS:StringField()
    departement:StringField()
    prix:FloatField()
    surface_habitable:IntField()
    DEPCOM:StringField()
    geometry:GeoPointField()
    id_transaction:StringField()
    id_ville:IntField()
    IRIS:StringField()
    latitude:StringField()
    longitude:StringField()
    n_pieces:IntField()
    NOM_COM:StringField()
    NOM_IRIS:StringField()
    TYPE_IRIS:StringField()
    type_batiment:StringField()
    vefa:BooleanField()
    ville:StringField()

