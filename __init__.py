from app.utils.PluginClass import PluginClass
from flask_jwt_extended import jwt_required, get_jwt_identity
from celery import shared_task
from app.utils import DatabaseHandler
from app.api.records.models import RecordUpdate
import os
from flask import request
import spacy

mongodb = DatabaseHandler.DatabaseHandler()
WEB_FILES_PATH = os.environ.get('WEB_FILES_PATH', '')
ORIGINAL_FILES_PATH = os.environ.get('ORIGINAL_FILES_PATH', '')
plugin_path = os.path.dirname(os.path.abspath(__file__))
models_path = plugin_path + '/models'

class ExtendedPluginClass(PluginClass):
    def __init__(self, path, import_name, name, description, version, author, type, settings):
        super().__init__(path, __file__, import_name, name,
                         description, version, author, type, settings)

    def add_routes(self):
        @self.route('/bulk', methods=['POST'])
        @jwt_required()
        def bulk_processing():
            current_user = get_jwt_identity()
            body = request.get_json()

            if 'post_type' not in body:
                return {'msg': 'No se especificó el tipo de contenido'}, 400

            if not self.has_role('admin', current_user) and not self.has_role('processing', current_user):
                return {'msg': 'No tiene permisos suficientes'}, 401
            
            task = self.bulk.delay(body, current_user)
            self.add_task_to_user(task.id, 'anonimizacionJep.bulk', current_user, 'msg')
            
            return {'msg': 'Se agregó la tarea a la fila de procesamientos'}, 201
        
        @self.route('/anomgenerate', methods=['POST'])
        @jwt_required()
        def anomgenerate():
            current_user = get_jwt_identity()
            body = request.get_json()

            if 'post_type' not in body:
                return {'msg': 'No se especificó el tipo de contenido'}, 400

            if not self.has_role('admin', current_user) and not self.has_role('processing', current_user):
                return {'msg': 'No tiene permisos suficientes'}, 401
            
            task = self.anom.delay(body, current_user)
            self.add_task_to_user(task.id, 'anonimizacionJep.anomgenerate', current_user, 'msg')
            
            return {'msg': 'Se agregó la tarea a la fila de procesamientos'}, 201
        
    @shared_task(ignore_result=False, name='anonimizacionJep.bulk')
    def bulk(body, user):
        def get_word_number(text, start, end):
            words = text.split()
            word_numbers = []
            current_index = 0

            for i, word in enumerate(words):
                word_length = len(word)
                if current_index <= start <= current_index + word_length or current_index <= end <= current_index + word_length:
                    word_numbers.append(i)
                current_index += word_length + 1

            return word_numbers

        filters = {
            'post_type': body['post_type']
        }

        if 'parent' in body:
            if body['parent']:
                filters['parents.id'] = body['parent']

        # obtenemos los recursos
        resources = list(mongodb.get_all_records(
            'resources', filters, fields={'_id': 1}))
        resources = [str(resource['_id']) for resource in resources]

        records_filters = {
            'parent.id': {'$in': resources},
            'processing.fileProcessing': {'$exists': True},
            'processing.ocrProcessing': {'$exists': True},
            '$or': [{'processing.fileProcessing.type': 'document'}]
        }
        if body['overwrite']:
            records_filters['processing.textDocumentAnom'] = {'$exists': True}
        else:
            records_filters['processing.textDocumentAnom'] = {
                '$exists': False}

        records = list(mongodb.get_all_records('records', records_filters, fields={
            '_id': 1, 'mime': 1, 'filepath': 1, 'processing': 1}))
        
        if len(records) == 0:
            return {'msg': 'No hay registros para procesar'}, 404
        
        path_ = os.path.join(models_path, 'spacyModelCEV_2022')
        nlp = spacy.load(path_)

        for record in records:
            result_ocr = record['processing']['ocrProcessing']['result']
            for r in result_ocr:
                blocks = r['blocks']
                for b in blocks:
                    entities = []
                    if 'text' not in b:
                        continue
                    text = b['text']

                    text_nlp = nlp(text)
                    for ent in text_nlp.ents:
                        entities.append({
                            "label":[ent.label_],
                            "text": ent.text,
                            "points": [
                                {
                                    "start": ent.start_char,
                                    "end": ent.end_char
                                }
                            ]
                        })

                    if len(entities) > 0:
                        
                        for ent in entities:
                            num = get_word_number(text, ent['points'][0]['start'], ent['points'][0]['end'])

                            if len(num) == 1:
                                b['words'][num[0]]['label'] = ent['label']
                            elif len(num) == 2:
                                for x in range(num[0], num[1] + 1):
                                    b['words'][x]['label'] = ent['label']

                    for w in b['words']:
                        if 'label' not in w:
                            w['label'] = ['O']
            update = {
                'processing': record['processing']
            }

            update['processing']['textDocumentAnom'] = {
                'type': 'anomt_extraction',
                'result': record['processing']['ocrProcessing']['result']
            }

            update = RecordUpdate(**update)
            mongodb.update_record(
                'records', {'_id': record['_id']}, update)
        
        return 'Hello, World!'
        
    @shared_task(ignore_result=False, name='anonimizacionJep.anomgenerate')
    def anom(body, user):

        return 'Hello, World!'

plugin_info = {
    'name': 'Extracción y anonimización de entidades en documentos PDF',
    'description': 'Plugin para extraer entidades nombradas y generar una versión anonimizada de un texto en un documento PDF.',
    'version': '0.1',
    'author': 'Néstor Andrés Peña',
    'type': ['bulk'],
    'settings': {
        'settings_bulk': [
            {
                'type':  'instructions',
                'title': 'Instrucciones',
                'text': 'Este plugin genera una versión anonimizada de los archivos transcritos.',
            },
            {
                'type': 'checkbox',
                'label': 'Sobreescribir procesamientos existentes',
                'id': 'overwrite',
                'default': False,
                'required': False,
            }
        ],
        'settings_detail': [
            {
                'type': 'radio',
                'label': 'Entidades nombradas a extraer',
                'id': 'type',
                'default': 'anom',
                'required': True,
                'options': [
                    {
                        'label': 'Personas',
                        'value': 'PER'
                    },
                    {
                        'label': 'Organizaciones',
                        'value': 'ORG'
                    },
                    {
                        'label': 'Lugares',
                        'value': 'LOC'
                    },
                    {
                        'label': 'Fechas',
                        'value': 'DATE'
                    }
                ]
            },
            {
                'type': 'button',
                'label': 'Generar versión anonimizada',
                'id': 'generate',
                'callback': 'anomgenerate'
            }
        ]
    }
}
