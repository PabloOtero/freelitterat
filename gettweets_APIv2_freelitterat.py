# -*- coding: utf-8 -*-
"""
Created on Wed May 29 09:24:07 2024

@author: pablo.otero@ieo.es
         Instituto Español de Oceanografía
"""


import requests
import os
import json
import pandas as pd
import re
from twarc.client2 import Twarc2
from twarc.expansions import ensure_flattened
from datetime import datetime, timedelta, timezone

import urllib.parse
from geopy.geocoders import Nominatim
import time
from pyxylookup import pyxylookup
from shapely.geometry import Point, Polygon, MultiPolygon
from nltk.corpus import stopwords
import geopy.distance
import pickle

from google.cloud import vision

import urllib



os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="client_secrets_google.json"

client = vision.ImageAnnotatorClient()



#search_url = "https://api.twitter.com/2/tweets/search/all"
# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
#bearer_token = os.environ.get("BEARER_TOKEN")
bearer_token = ""



def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UsageTweetsPython"
    return r

def connect_to_endpoint(url):
    response = requests.request("GET", url, auth=bearer_oauth)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()

def get_coords_type(saddress):
	try:
		geolocator = Nominatim(user_agent="your_email_account@gmail.com")
		location = geolocator.geocode(saddress, exactly_one=False, addressdetails=True)
		#print("C# %s %s  ---> %s" % (location.latitude, location.longitude, location.address))
		#print(location.raw)
		return location.latitude, location.longitude, location.raw['type']
	except:		
		return None	


def get_osm_location(saddress):
	try:
		geolocator = Nominatim(user_agent="your_email_account@gmail.com")
		location = geolocator.geocode(saddress, exactly_one=False, addressdetails=True, country_codes='es')
		return location
	except:		
		return None	
    
def get_osm_location_all(saddress):
	try:
		geolocator = Nominatim(user_agent="your_email_account@gmail.com")
		location = geolocator.geocode(saddress, exactly_one=False, addressdetails=True)
		return location
	except:		
		return None	    

def city_province_state_country(coord):
    try:
        geolocator = Nominatim(user_agent="your_email_account@gmail.com")
        location = geolocator.reverse(coord, exactly_one=True)
        if None in location:
            address = ''
            city = ''
            county = ''
            province = ''
            state = ''
            country = ''
            country_code = ''
        else:    
            address = location.raw['address']
            try:
                city = address.get('city', '')
            except:
                pass
            try:
                county = address.get('county', '')
            except:
                pass
            try:
                province = address.get('province', '')
            except:
                pass
            try:
                state = address.get('state', '')
            except:
                pass
            try:
                country = address.get('country', '')
            except:
                pass
            try:
                country_code = address.get('country_code', '')
            except:
                pass
        return city, county, province, state, country, country_code
    except:
        return None	



#Gets the text, removes links, hashtags, mentions, media, and symbols.
def get_text_cleaned_api2(status):
    """
    status is the object obtained after calling the Twitter API
    """
    
    #text = status['text']
    text = status['text']
    
    slices = []
    #Strip out the urls.
    try:
        if isinstance(status['entities.urls'], list):
            for url in status['entities.urls']:
                slices += [{'start': url['start'], 'stop': url['end']}]
    except:
        pass
    
    #Strip out the hashtags?
    # In this case, we want to convert the hashtags to words. Example:
    # 'encontré esto en la #playa de #suances'
    # if isinstance(status['entities.hashtags'], list): 
    #     for tag in status['entities.hashtags']:
    #         slices += [{'start': tag['start'], 'stop': tag['end']}]
    text = text.replace('#','')
    
    #Strip out the user mentions.
    try:
        if isinstance(status['entities.mentions'], list): 
            for men in status['entities.mentions']:
                slices += [{'start': men['start'], 'stop': men['end']}]
        
        #Strip out the media.
        #if 'media' in status['entities']:
        #    for med in status['entities']['media']:
        #        slices += [{'start': med['indices'][0], 'stop': med['indices'][1]}]
    except:
        pass
    
    #Strip out the symbols.
    try:
        if 'symbols' in status['entities']:
            for sym in status['entities']['symbols']:
                slices += [{'start': sym['indices'][0], 'stop': sym['indices'][1]}]
    except:
        pass
    
    # Sort the slices from highest start to lowest.
    slices = sorted(slices, key=lambda x: -x['start'])
    
    #No offsets, since we're sorted from highest to lowest.
    for s in slices:
        text = text[:s['start']] + text[s['stop']:]
        
    return text

#Sanitizes the text by removing front and end punctuation, 
#making words lower case, and removing any empty strings.
"""
def get_text_sanitized(status):    
    return ' '.join([w.lower().strip().rstrip(string.punctuation)\
        .lstrip(string.punctuation).strip()\
        for w in get_text_cleaned(status).split()\
        if w.strip().rstrip(string.punctuation).strip()])
"""

def get_text_sanitized(status):    
    #or RT sign in the beginning of the tweet
    
    tweet = get_text_cleaned_api2(status)
    
    tweet = re.sub(r'RT:', '', tweet)
    tweet = re.sub(r'@', '', tweet)
   
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    tweet = emoji_pattern.sub(r'', tweet)
 
    url_pattern = re.compile(r'https?://\S+|www\.\S+')
    tweet = url_pattern.sub('', tweet)
       

    #normalize case
    tweet = tweet.lower()
    
    tweet = tweet.replace('\u200d',' ')
    
    tweet = tweet.replace('.',' ').replace(',',' ').replace('/',' ').replace(';',' ').replace(':',' ')
    
    tweet = tweet.replace('?',' ').replace('!',' ').replace('¿',' ').replace('¡',' ').replace('(',' ').replace(')',' ')
  
    #avoid problems with japanese characters
    tweet = tweet.replace("\r"," ").replace("\n", " ")
    
    #remove excess spaces
    tweet = re.sub(' +', ' ', tweet)
     
    return tweet


def remove_tweet_out_polygon(df_filtered):
    """
    Discard tweets not located inside a predefined polygon
    """
    polygon_canarias = Polygon([(-18.3, 27.5), (-13.2, 27.5), (-13.2, 29.5), (-18.3, 29.5), (-18.3, 27.5)])
    polygon_peninsula_baleares = Polygon([(-9.5, 41.8), (-7.5,41.8), (-7.5, 36.47), (-5.25, 35.75), (-1.47, 36.25), (5.52, 40.0), (3.54, 42.45), (-1.5, 44.0), (-9.5, 44.0), (-9.5, 41.8)])
    multipolygon = MultiPolygon([polygon_canarias, polygon_peninsula_baleares])
    
    for index, row in df_filtered.iterrows():        
        try:
           location_tweet = [row['tweet_coords'][0], row['tweet_coords'][1]] 
           point = Point(location_tweet)
           if(point.within(multipolygon)):
               continue
           else:
               print('Out the bounding box -> Discard tweet ', row['id'])
               df_filtered = df_filtered.drop([index])
        except:
            continue
          
    return df_filtered

def remove_tweet_out_ie_uk(df_filtered):
    """
    Discard tweets not located inside a predefined polygon
    """  
    polygon_ireland_uk = Polygon([(-16, 49.5), (2, 49.5), (-3.5, 60), (-16, 49.5)])
      
    for index, row in df_filtered.iterrows():        
        try:
           location_tweet = [row['tweet_coords'][0], row['tweet_coords'][1]] 
           point = Point(location_tweet)
           if(point.within(polygon_ireland_uk)):
               continue
           else:
               print('Out the bounding box -> Discard tweet ', row['id'])
               df_filtered = df_filtered.drop([index])
        except:
            continue
          
    return df_filtered


def remove_tweet_out_fr(df_filtered):
    """
    Discard tweets not located inside a predefined polygon
    """  
    polygon_fr = Polygon([(-5.42, 51.25), (7.65, 51.15), (7.65, 42.23), (-5.42, 42.23), (-5.42,51.25)])
      
    for index, row in df_filtered.iterrows():        
        try:
           location_tweet = [row['tweet_coords'][0], row['tweet_coords'][1]] 
           point = Point(location_tweet)
           if(point.within(polygon_fr)):
               continue
           else:
               print('Out the bounding box -> Discard tweet ', row['id'])
               df_filtered = df_filtered.drop([index])
        except:
            continue
          
    return df_filtered


def remove_tweet_out_pt(df_filtered):
    """
    Discard tweets not located inside a predefined polygon
    """
    polygon_portugal = Polygon([(-9.7, 41.40), (-7.38, 41.40), (-7.38, 36.85), (-9.7, 36.85), (-9.7, 41.40)])
    polygon_illas = Polygon([(-32, 40.23), (-15.5,40.23), (-15.5, 32.05), (-32, 32.05), (-32,40.23)])

    multipolygon = MultiPolygon([polygon_portugal, polygon_illas])
    
    for index, row in df_filtered.iterrows():        
        try:
           location_tweet = [row['tweet_coords'][0], row['tweet_coords'][1]] 
           point = Point(location_tweet)
           if(point.within(multipolygon)):
               continue
           else:
               print('Out the bounding box -> Discard tweet ', row['id'])
               df_filtered = df_filtered.drop([index])
        except:
            continue
          
    return df_filtered



def discard_gifs(df_filtered):
    # Get media info and store in different columns
    # Discard animated gifs usually associated with memes 
    pd.set_option('mode.chained_assignment', None)
    
    for index, row in df_filtered.iterrows():
        if type(row['attachments.media']) is not list:
            df_filtered = df_filtered.drop([index])
        else:
            media_info = row['attachments.media'][0]
            df_filtered.media_type[index] = media_info['type']
            if media_info['type'] == 'photo':
                df_filtered.media_url[index] = media_info['url']
            elif media_info['type'] == 'animated_gif':
                df_filtered = df_filtered.drop([index])
            else:
                df_filtered.media_url[index] = media_info['preview_image_url']
                
    return df_filtered


def discard_unsafe_search(df_filtered):
    # Discard adult or spoof content
    
    # Names of likelihood from google.cloud.vision.enums
    likelihood_name = ('UNKNOWN', 'VERY_UNLIKELY', 'UNLIKELY', 'POSSIBLE',
                       'LIKELY', 'VERY_LIKELY')
    
    for index, row in df_filtered.iterrows():
        if type(row['attachments.media']) is not list:
            
            df_filtered = df_filtered.drop([index])
        else:
            media_info = row['attachments.media'][0]
            if media_info['type'] == 'photo':
                url = media_info['url']
                r = requests.get(url)
                if r.status_code == 200:               
                    content = urllib.request.urlopen(url).read()
                    image = vision.Image(content=content)
                    response = client.safe_search_detection(image=image)
                    safe = response.safe_search_annotation
                    if likelihood_name[safe.spoof] == 'LIKELY' or \
                        likelihood_name[safe.spoof] == 'VERY_LIKELY' or \
                        likelihood_name[safe.adult] == 'LIKELY' or \
                        likelihood_name[safe.adult] == 'VERY_LIKELY':
                        df_filtered = df_filtered.drop([index])
                time.sleep(0.5)      
                                   
    return df_filtered


def discard_in_response_to(df_filtered):
    for index, row in df_filtered.iterrows():
        if row['id'] != row['conversation_id']:
            df_filtered = df_filtered.drop([index])
            
    return df_filtered

def contains_match(text, matches):
    # Join the words in the matches list into a single regex pattern
    pattern = r'\b(?:' + '|'.join(re.escape(word) for word in matches) + r')\b'
    
    # Search for any of the words in the pattern within the text
    match = re.search(pattern, text, re.IGNORECASE)
    if match is not None:
        match = match.group(0)
    return match

def discard_similar_content(df_filtered):
    
    df_filtered['clean_text'] = None
    
    for index, row in df_filtered.iterrows():
        text = row['text']       
         
        slices = []
        #Strip out the urls.
        try:
            if isinstance(row['entities.urls'], list):
                for url in row['entities.urls']:
                    slices += [{'start': url['start'], 'stop': url['end']}]
                    # Sort the slices from highest start to lowest.
                    
                slices = sorted(slices, key=lambda x: -x['start'])
                
                #No offsets, since we're sorted from highest to lowest.
                for s in slices:
                    text = text[:s['start']] + text[s['stop']:]
                    
                df_filtered['clean_text'][index] = text
        except:
            pass
          
    df_filtered=df_filtered.drop_duplicates(subset=['clean_text'])
    
    return df_filtered
        
    

def discard_by_text(df_filtered):
    ''' Obtenidas mediante ChatGPT y otras por inspección del texto'''
    
    matches = ['noticias', 'noticia', 'periódico', 'informar', 'actualidad', 
                'prensa', 'jornal', 'periódicos', 'medios', 'política', 'político', 
                'elección', 'elecciones', 'candidato', 'candidatos', 
                'partido', 'partidos', 'votación', 'votaciones', 'campamento', 'campamentos', 
                'refugiado', 'refugiados', 'refugio', 'refugios', 'guerra', 'conflicto', 
                'conflictos', 'violencia', 'terrorismo', 'terrorista', 'ataque', 'ataques', 
                'militar', 'militares', 'ejército', 'armamento', 'arma', 'armas', 'bombas', 
                'explosión', 'explosiones', 'muerte', 'muertes', 'herido', 'heridos', 'desastre', 
                'desastres', 'accidente', 'accidentes', 'tragedia', 'tragedias', 'crimen', 'crímenes', 
                'criminales', 'homicidio', 'homicidios', 'robo', 'robos', 'fuego', 'incendio', 'incendios', 
                'terremoto', 'terremotos', 'tormenta', 'tormentas', 'huracán', 'huracanes', 'tormenta', 
                'tormentas', 'víctima', 'víctimas', 'crisis', 'epidemia', 'pandemia', 'enfermedad', 
                'enfermedades', 'medicina', 'médico', 'médica', 'hospital', 'hospitales', 
                'medicamento', 'medicamentos', 'covid', 'coronavirus', 'delta', 'omicron', 'variante', 
                'vacuna', 'vacunas', 'inmunización', 'contagio', 'infección', 'infectado', 'infectados', 
                'contagiado', 'contagiados', 'síntomas', 'aislamiento', 'cuarentena', 'tratamiento', 
                'logro', 'premio', 'premios', 'academia', 'oscar', 'grammy', 'nominación', 'nominaciones', 
                'canción', 'canciones', 'álbum', 'álbumes', 'música', 'músico', 'música', 'cine', 'película', 
                'películas', 'actor', 'actores', 'actriz', 'actrices', 'director', 'directora', 'producción', 
                'estreno', 'estrenos', 'premier', 'tráiler', 'galardón', 'galardones', 'cinematográfico', 
                'cineasta', 'taquilla', 'escenario', 'drama', 'comedia', 
                'romance', 'suspenso', 'terror', 'ciencia ficción', 'fantasía', 'thriller', 'documental', 
                'infantil', 'animación', 'adulto', 'pornografía', 'sexual', 'sexo', 'erótico', 'erótica', 
                'nudista', 'adulta', 'escena', 'escenas', 'sensual', 'desnudo', 'desnuda', 'nudez', 'nudismo', 
                'erógeno', 'orgía', 'kamasutra', 'lujuria', 'pasión', 'intimidad', 'sexualidad', 'fetiche', 
                'provocativo', 'provocativa', 'bikini', 'biquini', 'caliente', 'ardiente', 'excitante', 'picante', 
                'seducir', 'seducción', 'seductor', 'seductora', 'amante', 'amantes', 'amor', 'amoroso', 'amorosa', 
                'romántico', 'romántica', 'coquetear', 'coqueteo', 'coqueto', 'coqueta', 'orgasmo', 'orgásmico', 
                'eréctil', 'excitación', 'excitar', 'estimulante', 'placer', 'placeres', 'cama', 'desnudarse', 
                'desnudarse', 'masturbación', 'masturbarse', 'vibrador', 'juguetes sexuales', 'juguetes íntimos', 
                'pene', 'vagina', 'genitales', 'testículos', 'pechos', 'senos', 'trasero', 
                'penetración', 'felación', 'cunnilingus', 'posiciones sexuales', 'sexualidad', 'sexuales', 
                'lencería', 'fetiche', 'fetichismo', 'bdsm', 'bondage', 'dominación', 'sumisión', 'sadomasoquismo', 
                'fetiche', 'fetichista', 'xxx', 'porn', 'hardcore', 'desnudos', 'desnudez', 'intercambio de pareja', 
                'trío', 'orgía', 'swinger', 'swingers', 'swinging', 'gay', 'lesbiana', 'transexual', 'bisexual', 
                'travesti', 'transgénero', 'género', 'homosexual', 'homosexualidad', 'bisexualidad', 'transgénero', 
                'travestismo', 'transexualidad', 'gayfriendly', 'porno', 'pornográfico', 'pornografía', 'erótico', 
                'erótica', 'sexshop', 'juguetes eróticos', 'condones', 'preservativo', 'anticonceptivo', 
                'anticonceptivos', 'aborto', 'abortista', 'aborto legal', 'aborto ilegal', 'prostitución', 
                'prostituta', 'prostitución', 'prostituirse', 'prostíbulo', 'puta', 'putas', 'prostíbulo', 
                'tráfico sexual', 'trata de personas', 'pornografía infantil', 'pedofilia', 'abusos sexuales', 
                'abusador', 'abusadora', 'abusadores', 'abusadoras', 'violación', 'violador', 'violadora', 
                'violadores', 'violadoras', 'violencia de género', 'acoso', 'acosador', 'acosadora', 'acosadores', 
                'acosadoras', 'acosar', 'hostigamiento', 'hostigar', 'discriminación', 'discriminar', 'discriminatorio', 
                'discriminatoria', 'racial', 'racismo', 'racista', 'xenofobia', 'xenófobo', 'islamofobia', 'antisemitismo', 
                'odio', 'odiar', 'ofensivo', 'ofensiva', 'ofender', 'ofensa', 'ofensas', 'ofender', 'ofenderse', 'censura', 
                'censurar', 'censurable', 'censurado', 'censurada', 'censurados', 'censuradas', 'censurando', 'censuraron', 
                'censurará', 'censurarán', 'censurando', 'bloquear', 'bloqueo', 'bloqueado', 'bloqueada', 'bloqueados', 
                'bloqueadas', 'bloqueando', 'bloquearon', 'bloqueará', 'bloquearán', 'cierre', 'cerrar', 'cerrado', 'cerrada', 
                'cerrados', 'cerradas', 'cerrando', 'cerraron', 'cerrará', 'cerrarán', 'cerrando', 'limitación', 'limitar', 
                'limitado', 'limitada', 'limitados', 'limitadas', 'limitando', 'limitaron', 'limitará', 'limitarán', 'limitando', 
                'restricción', 'restringir', 'restringido', 'restringida', 'restringidos', 'restringidas', 'restringiendo', 
                'restringieron', 'restringirá', 'restringirán', 'restringiendo',
                'ecuatoriano', 'ecuatoriana', 'sexo', 'club', 'baños',
                'caliente', 'nudista', 'adopta', 'adoptanocompres', 'lealesorg',
                'buscando', 'cadáver', 'casa', 'desaparecido',
                'disfrutar', 'gatos', 'rica', 'sebusca', 'vacaciones', 'perdido',
                'a costa de', 'hot', 'perrito', 'culo', 'culito', 'bikini', 'biquini',
                'chabón', 'lover', 'polla', 'chocho', 'tiro', 'tiros', 'pistola', 
                'Amazon', 'compra', 'venta', 'lanzamiento', '#Culiacán', '#Sinaloa',
                'Patagonia', 'patagónico', '@LCajaDePandora', 'disco', 'canción', 'álbum',
                'concierto', 'música', 'fantasía', '#LaCajaDePandora', 'almendra',
                'escuchar', 'escuchando', 'cabrón', 'pendejo', 'cine', 'estrenos', 'estreno', 'homem',
                'gato', 'gatito', 'gatet',
                '💩', '😭', '😜', '🍆', '💦', '🍑', '😋', '🍻', '🍾', '♥️', '🔥', '🎶', '🎵', '⚽',
                'dinero', 'euros', '€', 'corpo', 'jeva', 'desconto', 'descuento', 'confira', 'Felipe',
                'Recife', 'Boa Viagem', 'bandera', 'distrito'] 
    
    matches=[x.lower() for x in matches]
    
    for index, row in df_filtered.iterrows():
        tweet = row['text'].lower()
        match = contains_match(tweet, matches)
        if match is not None:  
            print('Found undesired text in index ', str(index), ': ', match)
            df_filtered = df_filtered.drop([index])
            
    return df_filtered


def discard_by_text_fr(df_filtered):
    ''' Países de habla francesa, etc.'''
    
    matches = ['Paris', 'Seine', 'Benin', 'Ivory', 'Gabon', 'Guinea', 'Monaco', 'Niger', 'Senegal',
               'Togo', 'Quebec', 'Belgium', 'Burundi', 'Cameroon', 'Canada', 'Haiti', 'Mauritania',
               'Madagascar', 'Congo', 'Vanuatu', 'Canada', 'Espagne', 'Gaza', 'palestine', 'Luxembourg',
                '💩', '😭', '😜', '🍆', '💦', '🍑', '😋', '🍻', '🍾', '♥️', '🔥', '🎶', '🎵', '⚽'] 
    
    matches=[x.lower() for x in matches]
    
    for index, row in df_filtered.iterrows():
        tweet = row['text'].lower()
        match = contains_match(tweet, matches)
        if match is not None:  
            print('Found undesired text in index ', str(index), ': ', match)
            df_filtered = df_filtered.drop([index])
            
    return df_filtered

def discard_by_text_pt(df_filtered):
    ''' Países de habla portuguesa, playas en Barsil que no tienen nombre en Portugal, etc.'''
    
    matches = [
        "Copacabana", "Ipanema", "Leblon", "Jericoacoara",
        "Trancoso", "Fernando de Noronha", "Praia do Espelho", "Maragogi", "Boa Viagem",
        "Porto de Galinhas", "Maceió", "Natal", "Florianópolis",
        "Ilhabela", "Itacaré", "Arraial do Cabo", "Paraty", "Búzios",
        "Guarapari", "São Sebastião", "Ubatuba", "Ilha Grande",
        "Angra dos Reis", "Saquarema", "Camboriú", "Recife", "João Pessoa",
        "Salvador", "Vitória", "Aracaju", "São Luís", "Belém",
        "Arraial d'Ajuda", "Mangaratiba", "Macapá", "São Tomé das Letras",
        "Serra Grande", "Alcobaça", "Caravelas", "Cumuruxatiba",
        "Nova Viçosa", "Mucuri", "Itaúnas", "Conceição da Barra", "Guriri",
        "Barra Seca", "Jacaraípe", "Manguinhos", "Nova Almeida", "Itaparica",
        "Ponta Negra", "Tabatinga", "Pirangi do Norte", "Cumbuco", "Canoa Quebrada",
        "Morro Branco", "Majorlândia", "Genipabu", "Barra de São Miguel", "Gunga",
        "Pajuçara", "Ponta Verde", "Praia do Francês", "Coruripe",
        "Feliz Deserto", "Piaçabuçu", "Brejo Grande", "Ilha do Ferro", "Canindé de São Francisco",
        "Pirambu", "Pacatuba", "Japoatã", "Laranjeiras",
        "Santa Luzia", "Santo Amaro das Brotas", "Nossa Senhora do Socorro", "São Cristóvão",
        "Lagarto", "Simão Dias", "Poço Verde", "Tobias Barreto", "São Miguel dos Campos",
        "Roteiro", "Barra de Santo Antônio", "Paripueira", "Passo de Camaragibe",
        "Porto Calvo", "Porto de Pedras", "Japaratinga", "Maragogi", "São José da Coroa Grande",
        "Barreiros", "Tamandaré", "Rio Formoso", "Sirinhaém", "Rio Formoso",
        "São José da Coroa Grande", "São Miguel dos Milagres", "Porto de Pedras", "Maragogi",
        "Japaratinga", "Porto Calvo", "Porto de Pedras", "São Miguel dos Milagres",
        "Barra de Santo Antônio", "Paripueira", "Passo de Camaragibe", "São Miguel dos Milagres",
        "Porto de Pedras",
        "Angola", "Brazil","Cape Verde", "Timor", "Guinea", "Macau", "Mozambique", "São Tomé", "Príncipe",
        "💩", "😭", "😜", "🍆", "💦", "🍑", "😋", "🍻", "🍾", "♥️", "🔥", "🎶", "🎵", "⚽"] 

    
    matches=[x.lower() for x in matches]
    
    for index, row in df_filtered.iterrows():
        tweet = row['text'].lower()
        match = contains_match(tweet, matches)
        if match is not None:  
            print('Found undesired text in index ', str(index), ': ', match)
            df_filtered = df_filtered.drop([index])
            
    return df_filtered



def discard_by_country(df_filtered):
    paises_latinoamericanos = [
    "Argentina",
    "Bolivia",
    "Brasil",
    "Chile",
    "Colombia",
    "Costa Rica",
    "Cuba",
    "República Dominicana",
    "Ecuador",
    "El Salvador",
    "Guatemala",
    "Honduras",
    "México",
    "Nicaragua",
    "Panamá",
    "Paraguay",
    "Perú",
    "Puerto Rico",
    "Uruguay",
    "Venezuela"]
    
    paises_latinoamericanos_code = ['cl', 'co', 've', 'hn', 'mx', 'pe', 'ar', 'bo', 'br', 'cr', 'cu', 'ni', 'pa', 'py', 'ph', 'cn', 'gt', 'sv', 'gb', 'pg', 'pt', 'sv', 'tw', 'us', 'do']


    paises_latinoamericanos=[x.lower() for x in paises_latinoamericanos]
    
    for index, row in df_filtered.iterrows():
        tweet = row['text'].lower()
        match = contains_match(tweet, paises_latinoamericanos)
        if match is not None:  
            print('Found undesired text in index ', str(index), ': ', match)
            df_filtered = df_filtered.drop([index])

    for index, row in df_filtered.iterrows():
        try:
            tweet = row['author.location'].lower()
            match = contains_match(tweet, paises_latinoamericanos)
            if match is not None:  
                print('Found undesired text in index ', str(index), ': ', match)
                df_filtered = df_filtered.drop([index])
        except:
            pass

    for index, row in df_filtered.iterrows():
        try:
            tweet = row['author.description'].lower()
            match = contains_match(tweet, paises_latinoamericanos)
            if match is not None:  
                print('Found undesired text in index ', str(index), ': ', match)
                df_filtered = df_filtered.drop([index])                
        except:
            pass

    for index, row in df_filtered.iterrows():
        try:
            tweet = row['geo.country_code'].lower()
            match = contains_match(tweet, paises_latinoamericanos_code)
            if match is not None:  
                print('Found undesired text in index ', str(index), ': ', match)
                df_filtered = df_filtered.drop([index])                
        except:
            pass        

    #This exludes countries which are not in the Interreg region anthough keeps
    #nan values
    for index, row in df_filtered.iterrows():
        try:
            tweet = row['geo.country_code'].lower()
            match = contains_match(tweet, ['ES', 'IE', 'FR', 'PT'])
            if match is None:  
                 print('Found undesired text in index ', str(index), ': ', match)
                 df_filtered = df_filtered.drop([index])                
        except:
            pass
            
    return df_filtered



def discard_by_beach_name (df_filtered):
    ''' Obtenidas mediante ChatGPT'''
    
    matches = ['Akumal', 'Algodones', 'Anclote', 'Audiencia', 'Ayala',
                'Barra de Navidad', 'Barrita', 'Balandra', 'Boquita', 'Michoacán',
                'Bucerías', 'Puerto Peñasco', 'Caimancito', 'Carrizal', 'Carrizalillo',
                'Cerritos', 'Mazatlán', 'Todos Santos', 'Chacala',
                'Chileno', 'Corchos', 'Playa del Carmen', 'Playa Delfines', 'El Tecolote', 'Puerto Escondido',
                'Rosarito', 'Gemelas', 'Hacheras', 'Hadas', 'Playa Islitas', 'La Entrega',
                'La Ropa', 'Las Animas', 'Las Estacas', 'Las Gatas', 'Manzanillo',
                'Zihuatanejo', 'Mahahual', 'Maroma', 'Maviri', 'Miramar',
                'Puerto Vallarta', 'Sayulita', 'Isla Mujeres', 'Palancar',
                'Palmares', 'Colima', 'Ixtapa', 'Manzanillo', 'Pie de la Cuesta',
                'Pichilinguillo', 'Pichilingue',
                'Puerto Angelito', 'Puerto Ángel', 'Punta de Mita', 'Punta Negra', 'Punta Zicatela',
                'Requesón', 'Acapulco', 'Playa Saladita',
                'San Agustinillo', 'San Bruno', 'San Francisco', 'Alima', 'San Pancho',
                'Sayulita', 'Tamarindo', 'Tecolutla', 'Tecolote', 'Zihuatanejo',
                'Tizate', 'Troncones', 'Tulum', 'Tunco', 'Ventanilla', 'Ventura', 'Venados', 'Viudas',
                'Zipolite', 'Zipper', 'Zicatela', 'Zihuatanejo', 'Xcacel', 'Xpu-Ha', 'Chileno', 'Puerto Angelito',
                'Comodoro', 'Rivadavia', 'Atarraya', 'Bahía Bonita',
                'Balconada', 'Pehuen-Có', 'Ushuaia', 'Barda', 'Bristol', 'Mar del Plata',
                'Cantal', 'Trelew', 
                'Casita', 'Cauquén',
                'Cóndor',
                'Despensa', 'Puerto Madryn',
                'Embrujo', 'Emir',
                'Monte Hermoso', 'Tilly', 'Tierra del Fuego',
                'Fantasio', 'Gauchito', 'Necochea',
                'Hadas', 'Rawson', 'Honda', 'Hoya',
                'Larralde', 'Lobería',
                'Mangrullo', 'Mar de las Pampas', 'Mocha', 
                'Mujer Muerta', 'Pampas', 'Pascua', 'Pasarela', 
                'Patinaje', 'Patio', 'Perdices', 
                'Refugio', 
                'Rinconada',
                'Arica', 'Bahía Inglesa', 'Balneario Hornitos', 'Chonchi',
                'Valdivia', 'Bomba', 'Constitución', 'Lebu', 'Los Vilos', 'Pichilemu',
                'Bucalemu', 'Caleta Buena', 'Aceituno',
                'Chome', 'Cocholgüe', 'Coliumo', 'Chaihuín', 'Manzano', 'Quisco',
                'Calbuco', 'Juan Soldado', 'Llico', 'Lenga', 'Maitencillo',
                'Metri', 'Mechuque', 'Membrillo', 'Millagüe', 'Pelluco', 'Pichanco',
                'Pullao', 'Putun', 'Quintay', 'Riquelme', 'Tumbes', 'Zapallar',
                'Changa', 'Choros', 'Chucao', 'Cobquecura', 'Cole Cole', 'Sá da Costa', 'Recife',
                'Boa Viagem', 'Playa Blanca', 'Platja Blanca', 'Isla Treque']

    
    matches=[x.lower() for x in matches]
    
    for index, row in df_filtered.iterrows():
        #tweet = get_text_sanitized(row)
        tweet = row['text'].lower()
        if any(x in tweet for x in matches):       
            print('Found undesired beach name: ', [str(x) for x in matches if x in tweet])
            df_filtered = df_filtered.drop([index])
            
    return df_filtered

def locate_irish_uk_beach(df_filtered):
    
    beach_osm = pickle.load(open("beaches_ie_uk.pickle", "rb"))

    for index, row in df_filtered.iterrows():
        if row['gps_active'] == False:
            tweet = row['text'].lower()
            tweet = tweet.replace('#','')           
            key = [key for key, val in beach_osm.items() if key in tweet]
            val = [val for key, val in beach_osm.items() if key in tweet]            
            if len(key) > 0:
                key = key[0]
                val = val[0]               
                location_tweet = [val[0], val[1]]              
                df_filtered.tweet_coords[index] = location_tweet
                df_filtered.beach_name[index] = key
                df_filtered.openstreetmap_guess[index] = True
         
    return df_filtered

def locate_fr_beach(df_filtered):
    
    beach_osm = pickle.load(open("beaches_fr.pickle", "rb"))

    for index, row in df_filtered.iterrows():
        if row['gps_active'] == False:
            tweet = row['text'].lower()
            tweet = tweet.replace('#','')           
            key = [key for key, val in beach_osm.items() if key in tweet]
            val = [val for key, val in beach_osm.items() if key in tweet]            
            if len(key) > 0:
                key = key[0]
                val = val[0]               
                location_tweet = [val[0], val[1]]              
                df_filtered.tweet_coords[index] = location_tweet
                df_filtered.beach_name[index] = key
                df_filtered.openstreetmap_guess[index] = True
         
    return df_filtered

def locate_pt_beach(df_filtered):
    
    beach_osm = pickle.load(open("beaches_pt.pickle", "rb"))

    for index, row in df_filtered.iterrows():
        if row['gps_active'] == False:
            tweet = row['text'].lower()
            tweet = tweet.replace('#','')           
            key = [key for key, val in beach_osm.items() if key in tweet]
            val = [val for key, val in beach_osm.items() if key in tweet]            
            if len(key) > 0:
                key = key[0]
                val = val[0]               
                location_tweet = [val[0], val[1]]              
                df_filtered.tweet_coords[index] = location_tweet
                df_filtered.beach_name[index] = key
                df_filtered.openstreetmap_guess[index] = True
         
    return df_filtered





def discard_if_not_coastal(df_filtered):
    ''' Twitter está metiendo ahora en la búsqueda tweets no relacionados, lo que obliga a volver a filtrar'''
    
    matches = ['playa', 'praia', 'costa', 'cala', 'orilla', 'arena', 'area']
    
    for index, row in df_filtered.iterrows():
        tweet = row['text'].lower()
        
        found = any(word in tweet for word in matches)
        
        if not found:       
            print('Not coastal reference in the text: ', tweet)
            df_filtered = df_filtered.drop([index])
            
    return df_filtered

def discard_by_author_location_spain(df_filtered):
    matches = ['cl', 'co', 've', 'hn', 'mx', 'pe', 'ar', 'bo', 'br', 'cr', 'cu', 'ni', 'pa', 'py', 'ph', 'cn', 'gt', 'sv', 'gb', 'pg', 'pt', 'sv', 'tw', 'us', 'do']
    
    for index, row in df_filtered.iterrows():
        tweet = row['user_country_code']
        if tweet is not None:
            if any(x in tweet for x in matches):       
                print('Found undesired text: ', [str(x) for x in matches if x in tweet])
                df_filtered = df_filtered.drop([index])
            
    return df_filtered

def discard_by_author_location_ie_uk(df_filtered):
    matches = ['gb','ie']
    
    for index, row in df_filtered.iterrows():
        tweet = row['user_country_code']
        if (tweet is not None) and (tweet not in matches):   
            print('Found undesired country: ', tweet)
            df_filtered = df_filtered.drop([index])
            
    return df_filtered

def discard_by_author_location_fr(df_filtered):
    matches = ['fr']
    
    for index, row in df_filtered.iterrows():
        tweet = row['user_country_code']
        if (tweet is not None) and (tweet not in matches):   
            print('Found undesired country: ', tweet)
            df_filtered = df_filtered.drop([index])
            
    return df_filtered

def discard_by_author_location_pt(df_filtered):
    matches = ['pt']
    
    for index, row in df_filtered.iterrows():
        tweet = row['user_country_code']
        if (tweet is not None) and (tweet not in matches):   
            print('Found undesired country: ', tweet)
            df_filtered = df_filtered.drop([index])
            
    return df_filtered



def get_user_location_from_profile(df_filtered):
    # Get coordinates of the user from his/her profile. 
    # Note that 'nan' as string is a location in Italy
    # This step must be done previosuly to determine the tweet location beacuse
    # will be used later with comparison purposes
    #
    # Both valid and invalid locations are stores in a dictionary to avoid
    # multiple conecctions to the Nominatim API
    #
    try:   
        user_location = pickle.load(open("user_location.pickle", "rb"))
        print('Dictionary already exist')
    except:  
        user_location = dict()
        pickle.dump(user_location, open("user_location.pickle", "wb"))
        print('Dictionary does not exist')
    
    for index, row in df_filtered.iterrows():
        print(row['author.location']) 
        try:
           location = row['author.location'] 
           if pd.notnull(location) and (location != '') and (location != 'nan'):
               if location not in user_location:
                   print(location)
                   profile_coords = get_osm_location_all(location)
                   if len(profile_coords) > 1:
                       profile_coords = profile_coords[0]
                   # Give some time to the API    
                   time.sleep(0.5)      
                   try:
                       [city, county, province, state, country, country_code] = city_province_state_country([profile_coords[1][0], profile_coords[1][1]]) 
                       print('City: ' + city + ' State: ' + state + ' Country: ' + country + ' Code: ' + country_code)
                       df_filtered.user_country_code[index] = country_code
                       df_filtered.user_coords[index] = [profile_coords[1], profile_coords[0]]
                       # save to dict
                       #user_location = {location   : [country_code, profile_coords[1], profile_coords[0]]}
                       user_location[location] = [country_code, profile_coords[1][1], profile_coords[1][0]]                               
                   except:
                       user_location[location] = 'Invalid'
                       pass
               else:
                   if user_location[location] != 'Invalid':
                       df_filtered.user_country_code[index] = user_location[location][0]
                       df_filtered.user_coords[index] = [user_location[location][1], user_location[location][2]] 
                   else:
                       pass
        except:
            pass

    with open('user_location.pickle', 'wb') as handle:
        pickle.dump(user_location, handle, protocol=pickle.HIGHEST_PROTOCOL)    
      
    return df_filtered

def get_coords_from_device(df_filtered):   
    # Get coordinates from device or those estimated from geo.places
    # Tweets associated with Places are not necessarily issued from that location 
    # but could also potentially be about that location.
    for index, row in df_filtered.iterrows():         
        if ('geo.coordinates.coordinates' in row) and (isinstance(row['geo.coordinates.coordinates'], list)):
            location_tweet = [row['geo.coordinates.coordinates'][0], row['geo.coordinates.coordinates'][1]]   
            df_filtered.tweet_coords[index] = location_tweet
            df_filtered.gps_active[index] = True      
        elif ('geo.geo.bbox' in df_filtered) and (isinstance(row['geo.geo.bbox'], list)):
            location_tweet = [row['geo.geo.bbox'][0], row['geo.geo.bbox'][1]]
            df_filtered.tweet_coords[index] = location_tweet
            df_filtered.geoplace_active[index] = True
        else:
            continue
        
    return df_filtered


def get_coords_from_esri(df_filtered):
    # Try to guess coordinates of the beach from ESRI API
    # Here is the docs:     
    # https://opendata.esri.es/datasets/playas-espa%C3%B1olas/api
    # And here an example of url:
    # https://services1.arcgis.com/nCKYwcSONQTkPA4K/arcgis/rest/services/Playas_2015/FeatureServer/0/query?where=Nombre%20%3D%20%27AGUETE%27&outFields=Coordena_5,Nombre,Coordena_4&returnGeometry=false&outSR=4326&f=json
    # We must search by "playa de ...", note that without this preposition (de) is
    # easily misunderstood with the name of a residence place or touristic village
    # However, the input in the API must be the name of the beach without "de". 
    # For example, "Playa de A Lanzada" must be converted to "A%20Lanzada" 
    # Take into account:
    # "Lanzada" -> NOT valid
    # "La Lanzada" -> NOT valid
    # "A Lanzada" -> NOT valid
    # "La%20Lanzada" -> NOT valid
    # "A%20Lanzada" -> VALID
    #
    # Evaluates from more to less words. For example:
    # 1) playa de el caño seco HIGH PRIORITY
    # 2) playa de el caño
    # 3) playa de el           LOW PRIORITY
    #
    # And take into account that is sensitive to accents. For example: punta umbria
    # has no results from the API, but punta umbría. This must be properly encoded
    # 
    # We loose places as 'playa serena, roquetas del mar' due to the lack of 'de'
    #
    # We create a dict to store beaches and reduce connections to the API
    #
    
    try:   
        beach_esri = pickle.load(open("beach_esri.pickle", "rb"))
        print('Dictionary already exist')
    except:  
        beach_esri = dict()
        pickle.dump(beach_esri, open("beach_esri.pickle", "wb"))
        print('Dictionary does not exist')
    
    matches = [' playa de ', ' praia de ', ' platja de ', ' cala ']
    url_part1 = 'https://services1.arcgis.com/nCKYwcSONQTkPA4K/arcgis/rest/services/Playas_2015/FeatureServer/0/query?where=Nombre%20%3D%20%27'
    url_part2 = '%27&outFields=Coordena_5,Nombre,Coordena_4&returnGeometry=false&outSR=4326&f=json'       
    for index, row in df_filtered.iterrows():
        if df_filtered.tweet_coords[index] == None:
            tweet = get_text_sanitized(row)
            if any(x in tweet for x in matches): 
                beach_denomination = ''.join([str(x) for x in matches if x in tweet])
                try:
                    #tweet_words = tweet.split('playa de ')[1].strip().split()
                    tweet_words = tweet.split(beach_denomination)[1].strip().split()
                    nwords=3
                    while nwords > 0:
                        nombre_playa = ''
                        for iword in range(0,nwords):
                            nombre_playa = nombre_playa +  tweet_words[iword] + ' '
                        nwords -= 1
                        nombre_playa_original = nombre_playa.strip()
                        nombre_playa_urlparsed = urllib.parse.quote(nombre_playa_original)
                        url_playa = url_part1 + nombre_playa_urlparsed + url_part2
                                                             
                        if nombre_playa_original not in beach_esri:
                            try:
                                data, lat_playa, lon_playa = None, None, None
                                with urllib.request.urlopen(url_playa) as url:
                                    data = json.loads(url.read().decode('utf-8'))
                                if data and data['features']:
                                    lat_playa = data['features'][0]['attributes']['Coordena_5']
                                    lon_playa = data['features'][0]['attributes']['Coordena_4']
                                    df_filtered.tweet_coords[index] =  [lon_playa, lat_playa]
                                    df_filtered.beach_name[index] = nombre_playa_original
                                    df_filtered.esri_guess[index] = True
                                    beach_esri[nombre_playa_original] = [lon_playa, lat_playa]                               
                                    print(['ESRI API: ' + nombre_playa_original + ' ' + str(lat_playa) + ' ' + str(lon_playa)])
                                    break
                                else:
                                    beach_esri[nombre_playa_original] = 'Invalid'
                            except:                           
                                pass
                        else:
                            if beach_esri[nombre_playa_original] != 'Invalid':
                                df_filtered.tweet_coords[index] =  beach_esri[nombre_playa_original]
                                df_filtered.beach_name[index] = nombre_playa_original
                                df_filtered.esri_guess[index] = True
                                print(['ESRI Dictionary: ' + nombre_playa_original + ' ' + str(beach_esri[nombre_playa_original])])
                                break
                except:
                    pass            
    
    with open('beach_esri.pickle', 'wb') as handle:
        pickle.dump(beach_esri, handle, protocol=pickle.HIGHEST_PROTOCOL)    

    return df_filtered

def reverse_geocoding(df_filtered):    
    for index, row in df_filtered.iterrows():
        if df_filtered.tweet_coords[index] != None:
            try:                       
                # Give some time to the API    
                time.sleep(0.5)      
                [city, county, province, state, country, country_code] = city_province_state_country([df_filtered.tweet_coords[index][1], df_filtered.tweet_coords[index][0]]) 
                print('City: ' + city + ' State: ' + state + ' Country: ' + country + ' Code: ' + country_code)
                df_filtered.openstreetmap_city[index] = city
                df_filtered.openstreetmap_county[index] = county
                df_filtered.openstreetmap_province[index] = province
                df_filtered.openstreetmap_country_code[index] = country_code                                                        
            except:
                pass 
    return df_filtered

def get_coords_from_osm(df_filtered):
    # For those beaches not found in ESRI API, try it with OpenStreetmap. We cannot 
    # include it in the previous loop beacuse OpenStreetMap always try to post a 
    # response. For example, if we evaluate first "punta umbría desayuno" there is no
    # response from ESRI but Openstreetmap. We would lose the opportunity of
    # evaluating "punta umbría". Also note that ESRI exclusively evaluates Spanish
    # beaches (not all), however OpenStreetmap does it worldwide
    # Natural types from: https://wiki.openstreetmap.org/wiki/ES:Key:natural?uselang=es
    # Someone can writes "playa de Pobla de Farnals", however the correct address
    # in OSM is "Platja de Pobla de Farnals". This means that we must to find the
    # keyword (playa, platja, praia or cala) and test different options until 
    # find the desired reply. We also try to compose the longest possible name
    # for the beach:
    # playa de punta umbría
    # praia de punta umbría
    # platja de punta umbría
    # cala punta umbría
    # playa de punta
    # praia de punta
    # platja de punta
    # cala punta

    try:   
        beach_osm = pickle.load(open("beach_osm.pickle", "rb"))
        print('Dictionary already exist')
    except:  
        beach_osm = dict()
        pickle.dump(beach_osm, open("beach_osm.pickle", "wb"))
        print('Dictionary does not exist')

    
    natural_types = ['beach', 'cape', 'strait', 'coastline', 'reef', 'bay']
    matches = ['playa', 'praia', 'platja', 'cala', 'bahía']
    mystopwords = ['hace', 'puertorico', 'san', '"', 'tortugas', 'ría', 'canina', 'bahía', 'isla', 'en']
    mystopwords.extend(stopwords.words('spanish'))
    for index, row in df_filtered.iterrows():
        if row.tweet_coords == None:          
            tweet = get_text_sanitized(row)   
            #if any(x in tweet for x in matches):
            if contains_match(tweet,matches) != None:
                if 'platja de cala' in tweet: 
                    beach_name_options = ['platja de cala']
                elif 'playa de cala' in tweet:
                    beach_denomination = ['playa de cala']
                else:    
                    #beach_denomination = ''.join([str(x) for x in matches if x in tweet])
                    #beach_denomination = [str(x) for x in matches if x in tweet][0]
                    beach_denomination = contains_match(tweet,matches)
                    tweet_words = tweet.split(beach_denomination)[1].strip().split()
                    beach_name_options = []
                    nwords=min(len(tweet_words),4)
                    #reorder list to prioritize the way of user writing
                    #e.g. praia de Santa Catarina in Brazil
                    #     playa de Santa Catarina, Santa Cruz de Tenerife
                    custom_matches = matches.copy()
                    custom_matches.insert(0, custom_matches.pop(custom_matches.index(beach_denomination)))
                    while nwords > 0:
                        for x in custom_matches:  
                            if (tweet_words[nwords-1] not in mystopwords) and (not tweet_words[nwords-1].isdigit() ) and (len(tweet_words[nwords-1]) > 1 ):
                                nombre_playa = x +  ' ' + ' '.join(tweet_words[0:nwords])
                                beach_name_options.append(nombre_playa)
                        nwords -= 1
                    #else:
                    #    break
                for x in beach_name_options:
                    nombre_playa_original = x.strip()
                    print('OpenStreetMap EVAL:' + nombre_playa_original)
                    
                    if nombre_playa_original not in beach_osm:
                        try:
                            location = get_osm_location(nombre_playa_original)
                            coastal_locations = [ x for x in location if x.raw['type'] in natural_types ]
                            if len(coastal_locations) > 1:
                                print('Many coastal locations found')
                                coords_locations = [(loc.latitude, loc.longitude) for loc in coastal_locations]
                                if row.user_coords != None:
                                    print('Choosing closer point to user')
                                    distance_to_user = []
                                    coords_user = (row.user_coords[1], row.user_coords[0])
                                    for n in coords_locations:
                                        distance_to_user.append(geopy.distance.GeodesicDistance(n, coords_user).km)
                                    index_sorter_distance = distance_to_user.index(min(distance_to_user))
                                    location_tweet = [coords_locations[index_sorter_distance][1], coords_locations[index_sorter_distance][0]]
                                else:
                                    print('Choosing first result from OSM')
                                    location_tweet = [coords_locations[0][1], coords_locations[0][0]]
                                    location=location[0]
                            elif len(coastal_locations) == 1:
                                print('Only one coastal locations found')
                                location_tweet = [coastal_locations[0].longitude, coastal_locations[0].latitude]
                            else:
                                print('No coastal locations found. Adding invalid record to dict.')
                                beach_osm[nombre_playa_original] = 'Invalid'
                                continue
                            df_filtered.tweet_coords[index] = location_tweet
                            df_filtered.beach_name[index] = nombre_playa_original
                            df_filtered.openstreetmap_guess[index] = True
                            print(location_tweet)
                            break
                        except:
                            print('No coastal locations found. Adding invalid record to dict.')
                            beach_osm[nombre_playa_original] = 'Invalid'
                            continue                                                
                    else:
                        if beach_osm[nombre_playa_original] != 'Invalid':
                            df_filtered.tweet_coords[index] =  beach_osm[nombre_playa_original]
                            df_filtered.beach_name[index] = nombre_playa_original
                            df_filtered.openstreetmap_guess[index] = True
                            print(['OSM Dictionary: ' + nombre_playa_original + ' ' + str(beach_osm[nombre_playa_original])])
                            break


    with open('beach_osm.pickle', 'wb') as handle:
        pickle.dump(beach_osm, handle, protocol=pickle.HIGHEST_PROTOCOL)   
                                   
    return df_filtered


def exclude_non_located_tweets(df_filtered):
    # Tweet not located, user not located -> DISCARD
    # Tweet in Spain, user out Spain -> PASS
    # Tweet not located, user out Spain -> DISCARD
    # Tweet in Spain, user location unknown -> PASS
    # Habrá que evaluar qué casos de acierto/error tenemos después
    # Por ejemplo...ya le vale a un extremeño afincado en Madrid que pone que es de
    # "Cair Paravel" que es frikada de Crónicas de Narnia y también un barrio de 
    # Buenos Aires
    for index, row in df_filtered.iterrows():
        if (row['tweet_coords'] == None and row['user_country_code'] == None) or \
            (row['tweet_coords'] == None and row['user_country_code'] != 'es'):
            df_filtered = df_filtered.drop([index])

    return df_filtered


def compute_tweet_distances(df_filtered):
    
    #Distance onshore (negative) as limit 
    distance_onshore = -20
    
    # Shoredistance of tweets
    df_filtered['tweet_shoredistance'] = None
    for index, row in df_filtered.iterrows():
        if row['tweet_coords'] != None:
            try:
                locations = []
                locations.append(row['tweet_coords'])
                shoredistance = pyxylookup.lookup(locations, shoredistance=True, grids=False, areas=False, asdataframe=False)
                shoredistance = abs(shoredistance[0]['shoredistance'])/1000              
                if shoredistance > distance_onshore:
                    df_filtered.tweet_shoredistance[index] = shoredistance
                else:
                    df_filtered = df_filtered.drop([index])
            except:
                continue

    # Shoredistance of user profiles (user coastal knowledge)
    # How far the user lives from the coast?
    df_filtered['user_shoredistance'] = None
    for index, row in df_filtered.iterrows():
        if row['user_coords'] != None:
            try:
                locations = []
                locations.append(row['user_coords'])
                shoredistance = pyxylookup.lookup(locations, shoredistance=True, grids=False, areas=False, asdataframe=False)
                shoredistance = abs(shoredistance[0]['shoredistance'])/1000
                df_filtered.user_shoredistance[index] = shoredistance
            except:
                continue

    # How far from the tweet publication?...close? good knowledge
    df_filtered['user_tweet_distance'] = None
    for index, row in df_filtered.iterrows():
        if (row['tweet_coords'] != None and row['user_coords'] != None):
            try:        
                df_filtered.user_tweet_distance[index] = geopy.distance.GeodesicDistance(row['tweet_coords'], row['user_coords']).km
            except:
                continue
          
    return df_filtered


# 'source' label is not available in Twitter from Dec 2022 on

def reformat_df(df_filtered):
    
    if 'in_reply_to_user.id' not in df_filtered:
        df_filtered['in_reply_to_user.id'] = None
              
    if 'geo.full_name' not in df_filtered:
        df_filtered['geo.full_name'] = None
        
    if 'author.location' not in df_filtered:
        df_filtered['author.location'] = None
    
    df2 = df_filtered[['id',
                       'conversation_id',
                       'created_at',
                       'query_type',
                       'possibly_sensitive',
                       'text',
                       'lang',
                       'in_reply_to_user.id',
                       'media_type', 
                       'media_url', 
                       'tweet_coords', 
                       'gps_active', 
                       'geoplace_active',
                       'geo.full_name',
                       'beach_name', 
                       'esri_guess', 
                       'openstreetmap_guess',
                       'openstreetmap_natural',
                       'openstreetmap_city',
                       'openstreetmap_county',
                       'openstreetmap_province',
                       'tweet_shoredistance',
                       'author_id',
                       'author.username',
                       'author.location',
                       'user_coords',
                       'user_country_code',
                       'user_shoredistance',
                       'user_tweet_distance',
                       'public_metrics.retweet_count',
                       'public_metrics.quote_count',
                       'public_metrics.reply_count',
                       'public_metrics.like_count']]

    df2 = df2.rename(columns={"author.location": "author_location", 
                              "author.username": "author_username", 
                              "in_reply_to_user.id": "in_reply_to_user_id",
                              "geo.full_name": "geo_full_name",
                              "public_metrics.retweet_count": "retweet_count",
                              "public_metrics.quote_count": "quote_count",
                              "public_metrics.reply_count": "reply_count",
                              "public_metrics.like_count": "like_count",
                              })


    #to write properly original text in a new file
    for index, row in df2.iterrows():
        df2.text[index] = df2.text[index].replace("\r"," ").replace("\n", " ")
        
    return df2


def get_tweets_spain(queries, start_query_number, start_time=None, end_time=None):
    '''
    Get recent tweets from the search API

    Parameters
    ----------
    queries : list of arrays with queries
        DESCRIPTION.
    start_query_number: an int number to get control of the type of query
    start_time : datetime object, optional
        DESCRIPTION. If None, the search is the last week
    stop_time :datetime object, optional
        DESCRIPTION. If None, the search is the last week

    Returns
    -------
    Dataframe with one tweet per row

    '''
    
    df_final_tweets = pd.DataFrame()
    
    for query_number, query_string in enumerate(queries):
        print('Query type: ', int(query_number+start_query_number))
    
        if start_time != None:
            try:
                search_results = t.search_recent(query=query_string, start_time=start_time, end_time=end_time, max_results=100)
            except:
                raise Exception('Cannot retrieve tweets. Check start and stop times')
        else:
            search_results = t.search_recent(query=query_string, max_results=100)
    
        df = []
        for page in search_results:
            # Do something with the page of results:
            # print(page)
            # or alternatively, "flatten" results returning 1 tweet at a time, with expansions inline:
            #for tweet in ensure_flatten(page)['data']:
            for tweet in ensure_flattened(page):
                print(tweet['id'])
                normalized = pd.json_normalize(tweet, max_level=3)
                df.append(normalized)
        
        
        if not len(df):
            print('List is empty!')     
        else:
                 
            df = pd.concat(df, ignore_index = True)
            
            df_filtered = df.copy()
            df_filtered['query_type'] = query_number + start_query_number
            df_filtered['media_type'] = None
            df_filtered['media_url'] = None
            df_filtered['tweet_coords'] = None
            df_filtered['gps_active'] = False
            df_filtered['geoplace_active'] = False
            df_filtered['beach_name'] = None
            df_filtered['esri_guess'] = False
            df_filtered['openstreetmap_guess'] = False
            df_filtered['openstreetmap_natural'] = None
            df_filtered['openstreetmap_city'] = None
            df_filtered['openstreetmap_county'] = None
            df_filtered['openstreetmap_province'] = None
            df_filtered['user_coords'] = None
            df_filtered['user_country_code'] = None
            
                                                                               
            df_filtered = discard_gifs(df_filtered)           
            df_filtered = discard_in_response_to(df_filtered)
            
            df_filtered = discard_similar_content(df_filtered)   
            
            df_filtered = discard_by_text(df_filtered)      
            df_filtered = discard_by_beach_name(df_filtered)
            
            
            df_filtered = discard_by_country(df_filtered)
            df_filtered = get_user_location_from_profile(df_filtered)
            df_filtered = discard_by_author_location_spain(df_filtered)            
            df_filtered = get_coords_from_device(df_filtered)    
            df_filtered = get_coords_from_esri(df_filtered)
            df_filtered = get_coords_from_osm(df_filtered)
            df_filtered = remove_tweet_out_polygon(df_filtered)
                    
            # df_filtered = exclude_non_located_tweets(df_filtered)                   
            try:         
                df_filtered = discard_unsafe_search(df_filtered)
            except:
                pass
            
            df_filtered = compute_tweet_distances(df_filtered)   
            df_filtered = reverse_geocoding(df_filtered)    
            
            df2 = reformat_df(df_filtered)
            
            df_final_tweets = df_final_tweets.append(df2) 
            
        # To work properly, wait some seconds between requests
        time.sleep(60)
              
    return df_final_tweets


def get_tweets_ie_uk(queries, start_query_number, start_time=None, end_time=None):
    '''
    Get recent tweets from the search API

    Parameters
    ----------
    queries : list of arrays with queries
        DESCRIPTION.
    start_query_number: an int number to get control of the type of query
    start_time : datetime object, optional
        DESCRIPTION. If None, the search is the last week
    stop_time :datetime object, optional
        DESCRIPTION. If None, the search is the last week

    Returns
    -------
    Dataframe with one tweet per row

    '''
    
    df_final_tweets = pd.DataFrame()
    
    for query_number, query_string in enumerate(queries):
        print('Query type: ', int(query_number+start_query_number))
    
        if start_time != None:
            try:
                search_results = t.search_recent(query=query_string, start_time=start_time, end_time=end_time, max_results=100)
            except:
                raise Exception('Cannot retrieve tweets. Check start and stop times')
        else:
            search_results = t.search_recent(query=query_string, max_results=100)
    
        df = []
        for page in search_results:
            # Do something with the page of results:
            # print(page)
            # or alternatively, "flatten" results returning 1 tweet at a time, with expansions inline:
            #for tweet in ensure_flatten(page)['data']:
            for tweet in ensure_flattened(page):
                print(tweet['id'])
                normalized = pd.json_normalize(tweet, max_level=3)
                df.append(normalized)
        
        
        if not len(df):
            print('List is empty!')     
        else:
                 
            df = pd.concat(df, ignore_index = True)
            
            df_filtered = df.copy()
            df_filtered['query_type'] = query_number + start_query_number
            df_filtered['media_type'] = None
            df_filtered['media_url'] = None
            df_filtered['tweet_coords'] = None
            df_filtered['gps_active'] = False
            df_filtered['geoplace_active'] = False
            df_filtered['beach_name'] = None
            df_filtered['esri_guess'] = False
            df_filtered['openstreetmap_guess'] = False
            df_filtered['openstreetmap_natural'] = None
            df_filtered['openstreetmap_city'] = None
            df_filtered['openstreetmap_county'] = None
            df_filtered['openstreetmap_province'] = None
            df_filtered['user_coords'] = None
            df_filtered['user_country_code'] = None
            
                                                                               
            df_filtered = discard_gifs(df_filtered)           
            df_filtered = discard_in_response_to(df_filtered)
            
            df_filtered = discard_similar_content(df_filtered)   
            
            df_filtered = discard_by_text(df_filtered)      

            df_filtered = get_user_location_from_profile(df_filtered)
            df_filtered = discard_by_author_location_ie_uk(df_filtered)            
                                     
            df_filtered = get_coords_from_device(df_filtered) 
            df_filtered = remove_tweet_out_ie_uk(df_filtered)
                                    
            df_filtered = locate_irish_uk_beach(df_filtered)                           
            try:         
                df_filtered = discard_unsafe_search(df_filtered)
            except:
                pass

            df_filtered = compute_tweet_distances(df_filtered)   
            df_filtered = reverse_geocoding(df_filtered)    
           
            df2 = reformat_df(df_filtered)
            
            df_final_tweets = df_final_tweets.append(df2) 
            
        # To work properly, wait some seconds between requests
        time.sleep(60)
              
    return df_final_tweets


def get_tweets_fr(queries, start_query_number, start_time=None, end_time=None):
    '''
    Get recent tweets from the search API

    Parameters
    ----------
    queries : list of arrays with queries
        DESCRIPTION.
    start_query_number: an int number to get control of the type of query
    start_time : datetime object, optional
        DESCRIPTION. If None, the search is the last week
    stop_time :datetime object, optional
        DESCRIPTION. If None, the search is the last week

    Returns
    -------
    Dataframe with one tweet per row

    '''
    
    df_final_tweets = pd.DataFrame()
    
    for query_number, query_string in enumerate(queries):
        print('Query type: ', int(query_number+start_query_number))
    
        if start_time != None:
            try:
                search_results = t.search_recent(query=query_string, start_time=start_time, end_time=end_time, max_results=100)
            except:
                raise Exception('Cannot retrieve tweets. Check start and stop times')
        else:
            search_results = t.search_recent(query=query_string, max_results=100)
    
        df = []
        for page in search_results:
            # Do something with the page of results:
            # print(page)
            # or alternatively, "flatten" results returning 1 tweet at a time, with expansions inline:
            #for tweet in ensure_flatten(page)['data']:
            for tweet in ensure_flattened(page):
                print(tweet['id'])
                normalized = pd.json_normalize(tweet, max_level=3)
                df.append(normalized)
        
        
        if not len(df):
            print('List is empty!')     
        else:
                 
            df = pd.concat(df, ignore_index = True)
            
            df_filtered = df.copy()
            df_filtered['query_type'] = query_number + start_query_number
            df_filtered['media_type'] = None
            df_filtered['media_url'] = None
            df_filtered['tweet_coords'] = None
            df_filtered['gps_active'] = False
            df_filtered['geoplace_active'] = False
            df_filtered['beach_name'] = None
            df_filtered['esri_guess'] = False
            df_filtered['openstreetmap_guess'] = False
            df_filtered['openstreetmap_natural'] = None
            df_filtered['openstreetmap_city'] = None
            df_filtered['openstreetmap_county'] = None
            df_filtered['openstreetmap_province'] = None
            df_filtered['user_coords'] = None
            df_filtered['user_country_code'] = None
            
                                                                               
            df_filtered = discard_gifs(df_filtered)           
            df_filtered = discard_in_response_to(df_filtered)
            
            df_filtered = discard_similar_content(df_filtered)   
            
            df_filtered = discard_by_text_fr(df_filtered)      

            df_filtered = get_user_location_from_profile(df_filtered)
            df_filtered = discard_by_author_location_fr(df_filtered)            
                                     
            df_filtered = get_coords_from_device(df_filtered) 
            df_filtered = remove_tweet_out_fr(df_filtered)
                                    
            df_filtered = locate_fr_beach(df_filtered)                           
            try:         
                df_filtered = discard_unsafe_search(df_filtered)
            except:
                pass
            
            df_filtered = compute_tweet_distances(df_filtered)   
            df_filtered = reverse_geocoding(df_filtered)   
             
            df2 = reformat_df(df_filtered)
            
            df_final_tweets = df_final_tweets.append(df2) 
            
        # To work properly, wait some seconds between requests
        time.sleep(60)
              
    return df_final_tweets

def get_tweets_pt(queries, start_query_number, start_time=None, end_time=None):
    '''
    Get recent tweets from the search API

    Parameters
    ----------
    queries : list of arrays with queries
        DESCRIPTION.
    start_query_number: an int number to get control of the type of query
    start_time : datetime object, optional
        DESCRIPTION. If None, the search is the last week
    stop_time :datetime object, optional
        DESCRIPTION. If None, the search is the last week

    Returns
    -------
    Dataframe with one tweet per row

    '''
    
    df_final_tweets = pd.DataFrame()
    
    for query_number, query_string in enumerate(queries):
        print('Query type: ', int(query_number+start_query_number))
    
        if start_time != None:
            try:
                search_results = t.search_recent(query=query_string, start_time=start_time, end_time=end_time, max_results=100)
            except:
                raise Exception('Cannot retrieve tweets. Check start and stop times')
        else:
            search_results = t.search_recent(query=query_string, max_results=100)
    
        df = []
        for page in search_results:
            # Do something with the page of results:
            # print(page)
            # or alternatively, "flatten" results returning 1 tweet at a time, with expansions inline:
            #for tweet in ensure_flatten(page)['data']:
            for tweet in ensure_flattened(page):
                print(tweet['id'])
                normalized = pd.json_normalize(tweet, max_level=3)
                df.append(normalized)
        
        
        if not len(df):
            print('List is empty!')     
        else:
                 
            df = pd.concat(df, ignore_index = True)
            
            df_filtered = df.copy()
            df_filtered['query_type'] = query_number + start_query_number
            df_filtered['media_type'] = None
            df_filtered['media_url'] = None
            df_filtered['tweet_coords'] = None
            df_filtered['gps_active'] = False
            df_filtered['geoplace_active'] = False
            df_filtered['beach_name'] = None
            df_filtered['esri_guess'] = False
            df_filtered['openstreetmap_guess'] = False
            df_filtered['openstreetmap_natural'] = None
            df_filtered['openstreetmap_city'] = None
            df_filtered['openstreetmap_county'] = None
            df_filtered['openstreetmap_province'] = None
            df_filtered['user_coords'] = None
            df_filtered['user_country_code'] = None
            
                                                                               
            df_filtered = discard_gifs(df_filtered)           
            df_filtered = discard_in_response_to(df_filtered)
            
            df_filtered = discard_similar_content(df_filtered)   
            
            df_filtered = discard_by_text_pt(df_filtered)      

            df_filtered = get_user_location_from_profile(df_filtered)
            df_filtered = discard_by_author_location_pt(df_filtered)            
                                     
            df_filtered = get_coords_from_device(df_filtered) 
            df_filtered = remove_tweet_out_pt(df_filtered)
                                    
            df_filtered = locate_pt_beach(df_filtered)                           
            try:         
                df_filtered = discard_unsafe_search(df_filtered)
            except:
                pass
            
            df_filtered = compute_tweet_distances(df_filtered)   
            df_filtered = reverse_geocoding(df_filtered)             
             
            df2 = reformat_df(df_filtered)
            
            df_final_tweets = df_final_tweets.append(df2) 
            
        # To work properly, wait some seconds between requests
        time.sleep(60)
              
    return df_final_tweets


# Your bearer token here
t = Twarc2(bearer_token=bearer_token)


# How to build a query?
# https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query#examples
#
# Search_results is a generator, max_results is max tweets per page, 100 max for recent search.
# Get just 1 page of results instead of iterating over everything in search_results:
#   for page in itertools.islice(search_results, 1)
# To get everything use:
#   for page in search_results:`
# If filter by media is not included in the initial query, can be filtered after
# collection:  df_filtered = df[df['attachments.media'].notna()]
#
# search_results = t.search_all(query=query1, start_time=start_time, end_time=end_time, max_results=100)
#
# If you have Basic or Pro access, your query can be 512 characters long for recent search endpoint. 
# Max of 60 queries in 15 minutes
# 
# If you make a general search, you should try to filter as much as possible to
# not pull an excess of tweets. You can also use emoticons in the filter: 🍆 💦 🍑 😋 🍻 🍾 ♥️ 
# Words to skip 'Rica', 'Mexico', 'Méjico', 'Argentina', 'sexo', 
#               'caliente', 'nudista', 'adopta', 'adoptanocompres', 'lealesorg',
#               'buscando', 'cadáver', 'casa', 'ciudad', 'contacto', 'desaparecido',
#               'disfrutar', 'gatos', 'rica', 'sebusca', 'vacaciones', 'perdido'
# As there are many words, they are also filtered after scrapping
#
# Some queries fail if they are not written in just one line.


''' GENERAL QUERIES '''

query1 = '(limpieza) (playa OR playas OR cala OR costa OR litoral) -(🍆 OR 💦 OR 🍑 OR 😋 OR 🍻 OR 🍾 OR 😜 OR 🤣 OR 💩) -(rica OR México OR Mejico OR Argentina OR Chile OR nudista OR sexo OR caliente OR hot) -is:retweet has:media'
query2 = '(cleanup OR clean-up OR "clean up") (beach OR beaches OR coast OR harbour OR strand OR bay OR cove) -🍆 -💦 -🍑 -😋 -🍻 -🍾 -😜 -🤣 -💩 -sex -hot -is:retweet has:media lang:en'
query3 = '(nettoyage OR #nettoyage OR "ramassage de déchets" OR "ramassage des déchets" OR "collecte de déchet" OR "collectes de déchets" OR "collectes des déchets") (côtes OR plage OR plages OR balnéaire OR #côtes OR #plage OR #plages) -🍆 -💦 -🍑 -😋 -🍻 -🍾 -😜 -🤣 -💩 -sex -hot -is:retweet has:media lang:fr'
query4 = '(limpeza OR #limpeza OR limpeça OR #limpeça) (costa OR #costa OR praia OR #praia OR praias OR #praias) -🍆 -💦 -🍑 -😋 -🍻 -🍾 -😜 -🤣 -💩 -sex -hot -is:retweet has:media lang:pt'



''' FREELITTERAT QUERIES ''' 

queries = [query1]

filename = 'data/FreeLitterAT_tweets.csv'
# If the file exists, then read the existing data from the CSV file to know the last record
if os.path.exists(filename):
    df = pd.read_csv(filename, header=0)
    df = df.sort_values(by='created_at')
    parsed_start_time = df['created_at'].iloc[-1]
    del df
    parsed_start_time = datetime.strptime(parsed_start_time[:19], "%Y-%m-%dT%H:%M:%S")
    # Convert the parsed datetime to UTC
    parsed_start_time = parsed_start_time.replace(tzinfo=timezone.utc)
    parsed_start_time = parsed_start_time + timedelta(0,60) 
    parsed_end_time = datetime.now(timezone.utc) + timedelta(minutes=-1)
    delta=parsed_end_time-parsed_start_time
    if delta.days < 6:
        print('Last record in less than 6 days')
        df_general =  get_tweets_spain(queries, start_query_number=1, start_time=parsed_start_time, end_time=parsed_end_time)
    else:
        print('Last record in more than 6 days')
        df_general =  get_tweets_spain(queries, start_query_number=1, start_time=None, end_time=None)
else:
    print('First query. Creating file.')
    df_general =  get_tweets_spain(queries, start_query_number=1, start_time=None, end_time=None)

if len(df_general):
    df_general = df_general.drop_duplicates(subset=['id'])
    df_general = df_general.sort_values(by='created_at')
    
    # Check API spent quota
    url = "https://api.twitter.com/2/usage/tweets"
    json_response = connect_to_endpoint(url)
    #print(json.dumps(json_response, indent=4, sort_keys=True))
    reset_day = json_response['data']['cap_reset_day']
    quota = json_response['data']['project_usage']
    quota = int(round(int(quota)/10000*100))
    
    potential_finding = []
    for index, row in df_general.iterrows():
        if row['tweet_coords'] != None:
            potential_finding.append(row)
    
    
    if os.path.exists(filename):
        csvFile = open(filename, 'a' ,encoding='utf-8', newline='\n')            
        df_general.to_csv(csvFile, mode='a', line_terminator='', index=False, header=False, encoding="utf-8")
    else:
        csvFile = open(filename, 'w' ,encoding='utf-8', newline='\n')            
        df_general.to_csv(csvFile, mode='w', line_terminator='', index=False, encoding="utf-8")
    csvFile.close()



queries = [query2]

filename = 'data/FreeLitterAT_tweets_ie.csv'
# If the file exists, then read the existing data from the CSV file to know the last record
if os.path.exists(filename):
    df = pd.read_csv(filename, header=0)
    df = df.sort_values(by='created_at')
    parsed_start_time = df['created_at'].iloc[-1]
    del df
    parsed_start_time = datetime.strptime(parsed_start_time[:19], "%Y-%m-%dT%H:%M:%S")
    # Convert the parsed datetime to UTC
    parsed_start_time = parsed_start_time.replace(tzinfo=timezone.utc)
    parsed_start_time = parsed_start_time + timedelta(0,60) 
    parsed_end_time = datetime.now(timezone.utc) + timedelta(minutes=-1)
    delta=parsed_end_time-parsed_start_time
    if delta.days < 6:
        print('Last record in less than 6 days')
        df_general =  get_tweets_ie_uk(queries, start_query_number=1, start_time=parsed_start_time, end_time=parsed_end_time)
    else:
        print('Last record in more than 6 days')
        df_general =  get_tweets_ie_uk(queries, start_query_number=1, start_time=None, end_time=None)
else:
    print('First query. Creating file.')
    df_general =  get_tweets_ie_uk(queries, start_query_number=1, start_time=None, end_time=None)

if len(df_general):
    df_general = df_general.drop_duplicates(subset=['id'])
    df_general = df_general.sort_values(by='created_at')
    
    # Check API spent quota
    url = "https://api.twitter.com/2/usage/tweets"
    json_response = connect_to_endpoint(url)
    #print(json.dumps(json_response, indent=4, sort_keys=True))
    reset_day = json_response['data']['cap_reset_day']
    quota = json_response['data']['project_usage']
    quota = int(round(int(quota)/10000*100))
    
    potential_finding = []
    for index, row in df_general.iterrows():
        if row['tweet_coords'] != None:
            potential_finding.append(row)
    
    
    if os.path.exists(filename):
        csvFile = open(filename, 'a' ,encoding='utf-8', newline='\n')            
        df_general.to_csv(csvFile, mode='a', line_terminator='', index=False, header=False, encoding="utf-8")
    else:
        csvFile = open(filename, 'w' ,encoding='utf-8', newline='\n')            
        df_general.to_csv(csvFile, mode='w', line_terminator='', index=False, encoding="utf-8")
    csvFile.close()

queries = [query3]

filename = 'data/FreeLitterAT_tweets_fr.csv'
# If the file exists, then read the existing data from the CSV file to know the last record
if os.path.exists(filename):
    df = pd.read_csv(filename, header=0)
    df = df.sort_values(by='created_at')
    parsed_start_time = df['created_at'].iloc[-1]
    del df
    parsed_start_time = datetime.strptime(parsed_start_time[:19], "%Y-%m-%dT%H:%M:%S")
    # Convert the parsed datetime to UTC
    parsed_start_time = parsed_start_time.replace(tzinfo=timezone.utc)
    parsed_start_time = parsed_start_time + timedelta(0,60) 
    parsed_end_time = datetime.now(timezone.utc) + timedelta(minutes=-1)
    delta=parsed_end_time-parsed_start_time
    if delta.days < 6:
        print('Last record in less than 6 days')
        df_general =  get_tweets_fr(queries, start_query_number=1, start_time=parsed_start_time, end_time=parsed_end_time)
    else:
        print('Last record in more than 6 days')
        df_general =  get_tweets_fr(queries, start_query_number=1, start_time=None, end_time=None)
else:
    print('First query. Creating file.')
    df_general =  get_tweets_fr(queries, start_query_number=1, start_time=None, end_time=None)

if len(df_general):
    df_general = df_general.drop_duplicates(subset=['id'])
    df_general = df_general.sort_values(by='created_at')
    
    # Check API spent quota
    url = "https://api.twitter.com/2/usage/tweets"
    json_response = connect_to_endpoint(url)
    #print(json.dumps(json_response, indent=4, sort_keys=True))
    reset_day = json_response['data']['cap_reset_day']
    quota = json_response['data']['project_usage']
    quota = int(round(int(quota)/10000*100))
    
    potential_finding = []
    for index, row in df_general.iterrows():
        if row['tweet_coords'] != None:
            potential_finding.append(row)
    
    
    if os.path.exists(filename):
        csvFile = open(filename, 'a' ,encoding='utf-8', newline='\n')            
        df_general.to_csv(csvFile, mode='a', line_terminator='', index=False, header=False, encoding="utf-8")
    else:
        csvFile = open(filename, 'w' ,encoding='utf-8', newline='\n')            
        df_general.to_csv(csvFile, mode='w', line_terminator='', index=False, encoding="utf-8")
    csvFile.close()

queries = [query4]

filename = 'data/FreeLitterAT_tweets_pt.csv'
# If the file exists, then read the existing data from the CSV file to know the last record
if os.path.exists(filename):
    df = pd.read_csv(filename, header=0)
    df = df.sort_values(by='created_at')
    parsed_start_time = df['created_at'].iloc[-1]
    del df
    parsed_start_time = datetime.strptime(parsed_start_time[:19], "%Y-%m-%dT%H:%M:%S")
    # Convert the parsed datetime to UTC
    parsed_start_time = parsed_start_time.replace(tzinfo=timezone.utc)
    parsed_start_time = parsed_start_time + timedelta(0,60) 
    parsed_end_time = datetime.now(timezone.utc) + timedelta(minutes=-1)
    delta=parsed_end_time-parsed_start_time
    if delta.days < 6:
        print('Last record in less than 6 days')
        df_general =  get_tweets_pt(queries, start_query_number=1, start_time=parsed_start_time, end_time=parsed_end_time)
    else:
        print('Last record in more than 6 days')
        df_general =  get_tweets_pt(queries, start_query_number=1, start_time=None, end_time=None)
else:
    print('First query. Creating file.')
    df_general =  get_tweets_pt(queries, start_query_number=1, start_time=None, end_time=None)

if len(df_general):
    df_general = df_general.drop_duplicates(subset=['id'])
    df_general = df_general.sort_values(by='created_at')
    
    # Check API spent quota
    url = "https://api.twitter.com/2/usage/tweets"
    json_response = connect_to_endpoint(url)
    #print(json.dumps(json_response, indent=4, sort_keys=True))
    reset_day = json_response['data']['cap_reset_day']
    quota = json_response['data']['project_usage']
    quota = int(round(int(quota)/10000*100))
    
    potential_finding = []
    for index, row in df_general.iterrows():
        if row['tweet_coords'] != None:
            potential_finding.append(row)
    
    
    if os.path.exists(filename):
        csvFile = open(filename, 'a' ,encoding='utf-8', newline='\n')            
        df_general.to_csv(csvFile, mode='a', line_terminator='', index=False, header=False, encoding="utf-8")
    else:
        csvFile = open(filename, 'w' ,encoding='utf-8', newline='\n')            
        df_general.to_csv(csvFile, mode='w', line_terminator='', index=False, encoding="utf-8")
    csvFile.close()



