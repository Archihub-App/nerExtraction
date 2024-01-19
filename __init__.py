from app.utils.PluginClass import PluginClass
from flask_jwt_extended import jwt_required, get_jwt_identity
from celery import shared_task
from app.utils import DatabaseHandler
from app.api.records.models import RecordUpdate
import os
from flask import request, send_file
import spacy
from PIL import Image, ImageDraw
import uuid
from bson.objectid import ObjectId
from dotenv import load_dotenv
load_dotenv()

mongodb = DatabaseHandler.DatabaseHandler()
WEB_FILES_PATH = os.environ.get('WEB_FILES_PATH', '')
ORIGINAL_FILES_PATH = os.environ.get('ORIGINAL_FILES_PATH', '')
USER_FILES_PATH = os.environ.get('USER_FILES_PATH', '')
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
            self.add_task_to_user(task.id, 'nerExtraction.bulk', current_user, 'msg')
            
            return {'msg': 'Se agregó la tarea a la fila de procesamientos'}, 201
        
        @self.route('/anomgenerate', methods=['POST'])
        @jwt_required()
        def anomgenerate():
            current_user = get_jwt_identity()
            body = request.get_json()

            if 'id' not in body:
                return {'msg': 'No se especificó el archivo'}, 400

            if not self.has_role('admin', current_user) and not self.has_role('processing', current_user):
                return {'msg': 'No tiene permisos suficientes'}, 401
            
            task = self.anom.delay(body, current_user)
            self.add_task_to_user(task.id, 'nerExtraction.anomgenerate', current_user, 'file_download')
            
            return {'msg': 'Se agregó la tarea a la fila de procesamientos'}, 201
        
        @self.route('/filedownload/<taskId>', methods=['GET'])
        @jwt_required()
        def file_download(taskId):
            current_user = get_jwt_identity()

            if not self.has_role('admin', current_user) and not self.has_role('processing', current_user):
                return {'msg': 'No tiene permisos suficientes'}, 401
            
            # Buscar la tarea en la base de datos
            task = mongodb.get_record('tasks', {'taskId': taskId})
            # Si la tarea no existe, retornar error
            if not task:
                return {'msg': 'Tarea no existe'}, 404
            
            if task['user'] != current_user and not self.has_role('admin', current_user):
                return {'msg': 'No tiene permisos para obtener la tarea'}, 401

            if task['status'] == 'pending':
                return {'msg': 'Tarea en proceso'}, 400

            if task['status'] == 'failed':
                return {'msg': 'Tarea fallida'}, 400

            if task['status'] == 'completed':
                if task['resultType'] != 'file_download':
                    return {'msg': 'Tarea no es de tipo file_download'}, 400
                
            path = USER_FILES_PATH + task['result']
            return send_file(path, as_attachment=True)
        
    @shared_task(ignore_result=False, name='nerExtraction.bulk')
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
        if not body['overwrite']:
            records_filters['processing.nerExtraction'] = {'$exists': False}

        records = list(mongodb.get_all_records('records', records_filters, fields={
            '_id': 1, 'mime': 1, 'filepath': 1, 'processing': 1}))
        
        if len(records) == 0:
            return {'msg': 'No hay registros para procesar'}, 404
        
        path_ = os.path.join(models_path, 'spacyModelCEV_2022')
        nlp = spacy.load(path_)

        for record in records:
            result_ocr = [*record['processing']['ocrProcessing']['result']]
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

            update['processing']['nerExtraction'] = {
                'type': 'ner_extraction',
                'result': record['processing']['ocrProcessing']['result']
            }

            update = RecordUpdate(**update)
            mongodb.update_record(
                'records', {'_id': record['_id']}, update)
        
        return 'Hello, World!'
        
    @shared_task(ignore_result=False, name='nerExtraction.anomgenerate')
    def anom(body, user):
        record = mongodb.get_record('records', {'_id': ObjectId(body['id'])}, fields={
            '_id': 1, 'mime': 1, 'filepath': 1, 'processing': 1
        })

        if 'processing' not in record:
            return {'msg': 'Registro no tiene procesamientos'}, 404
        if 'fileProcessing' not in record['processing']:
            return {'msg': 'Registro no tiene procesamiento de archivo'}, 404
        if 'nerExtraction' not in record['processing']:
            return {'msg': 'Registro no tiene procesamiento de extracción de entidades nombradas'}, 404

        path = WEB_FILES_PATH + '/' + \
                    record['processing']['fileProcessing']['path'] + '/web/big'
        path_original = ORIGINAL_FILES_PATH + '/' + \
                    record['processing']['fileProcessing']['path'] + '.pdf'
        
        files = os.listdir(path)

        folder_path = USER_FILES_PATH + '/' + user + '/nerExtractionAnom'
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        file_id = str(uuid.uuid4())

        pdf_path = folder_path + '/' + file_id + '.pdf'

        step = 0
        images = []

        pages = record['processing']['nerExtraction']['result']

        hidden_labels = [b['id'] for b in body['type']]

        for file in files:
            step += 1
            page = pages[step - 1]
            img = Image.open(os.path.join(path, file))
            draw = ImageDraw.Draw(img)

            for block in page['blocks']:
                if 'words' not in block:
                    continue
                for word in block['words']:
                    labels = word['label']
                    # if any of labels is in body['type']
                    if any(label in hidden_labels for label in labels):
                        # draw a rectangle around the word in the image, the coordinates are relative to the image and are x,y,width,height
                        x = word['bbox']['x'] * img.width
                        y = word['bbox']['y'] * img.height
                        w = word['bbox']['width'] * img.width
                        h = word['bbox']['height'] * img.height
                        draw.rectangle((x, y, x + w, y + h), fill='white')
                        # write the label in the image
                        draw.text((x + (w / 2), y + (h / 2)), word['label'][0], fill='black')


            images.append(img)
        

        images[0].save(
            pdf_path, "PDF" ,resolution=100.0, save_all=True, append_images=images[1:]
        )



        return '/' + user + '/nerExtractionAnom/' + file_id + '.pdf'

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
                'type': 'multicheckbox',
                'label': 'Entidades nombradas a extraer',
                'id': 'type',
                'default': [],
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
            },
            {
                'type': 'button',
                'label': 'Generar versión etiquetada',
                'id': 'generate',
                'callback': 'taggenerate'
            }
        ]
    }
}
