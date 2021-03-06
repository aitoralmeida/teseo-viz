# -*- coding: utf-8 -*-
"""
Created on Tue Feb 18 11:29:20 2014

@author: aitor
"""

import pickle
import gender
import difflib
import math


def load_config():

    config = {
          'user': 'foo',
          'password': 'bar',
          'host': '127.0.0.1',
          'database': 'teseo',
        }
        
    with open('pass.config', 'r') as inputfile:
        for i, line in enumerate(inputfile):
            if i == 0:
                config['user'] = line
            elif i == 1:
                config['password'] = line
            elif i > 1:
                break
            
    return config

def get_university_ids():
    import mysql.connector 
    config = load_config()
    cnx = mysql.connector.connect(**config)
    cursor_unis = cnx.cursor()
    cursor_unis.execute("SELECT id, name FROM university")
    result = {}
    for university in cursor_unis:
        result[university[0]] = university[1]
    cursor_unis.close()
    
    print result
    
def save_thesis_ids():
    import mysql.connector   
    config = load_config()
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    cursor.execute("SELECT id FROM thesis")
    result = set()
    for thesis_id in cursor:
        result.add(thesis_id[0])
    cursor.close()
    
    with open( "thesis_ids.p", "wb" ) as outfile:
        pickle.dump(result, outfile)
        
def load_thesis_ids():
    with open( "thesis_ids.p", "rb" ) as infile:
        result = pickle.load(infile)
    return result
    
def save_descriptors():
    import mysql.connector   
    config = load_config()
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    cursor.execute("SELECT id, text FROM descriptor")
    result = {}
    for descriptor in cursor:
        result[descriptor[0]] = descriptor[1]
    cursor.close()
    
    with open( "descriptors.p", "wb" ) as outfile:
        pickle.dump(result, outfile)
        
def load_descriptors():
    with open( "descriptors.p", "rb" ) as infile:
        result = pickle.load(infile)
    return result
        
def get_names():
    import mysql.connector   
    config = load_config()
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    cursor.execute("SELECT DISTINCT(first_name) FROM person")
    result = set()
    for name in cursor:
        first = str(name[0]).split(' ')[0]
        result.add(first)
    cursor.close()
        
    return list(result)
    
def save_name_genders():
    name_pool = get_names()
        
    result = {}
    bad_names = []
    
    chunk_size = 50
    total_chunks = len(name_pool)/chunk_size
    rest = len(name_pool)%chunk_size    
    
    for j in range(0, total_chunks):
        print '*******Chunk', j, '/', total_chunks
        names = []
        if j == total_chunks - 1:
            names = name_pool[j * chunk_size:total_chunks*chunk_size+rest]
        else:
            names = name_pool[j * chunk_size:(j+1)*chunk_size]
        
        gender_list = gender.getGenders(names) #gender, prob, count        
        
        for i, name in enumerate(names):
            infered_gender = gender_list[i][0]
            prob = float(gender_list[i][1])
            print name, infered_gender, prob
            if infered_gender == 'None' or prob < 0.6: 
                bad_names.append(name)
            result[name] = infered_gender
        
    with open( "genders.p", "wb" ) as outfile:
        pickle.dump(result, outfile) 
        
    with open( "badnames.p", "wb" ) as outfile:
        pickle.dump(bad_names, outfile) 
    
    return bad_names
       
def load_genders():
    with open( "genders.p", "rb" ) as infile:
        result = pickle.load(infile)
    return result
    
def get_complete_names():
    import mysql.connector   
    config = load_config()
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    cursor.execute("SELECT DISTINCT(name) FROM person")
    result = []
    for name in cursor:
        result.append(name[0])
    cursor.close()
    return result
    
def check_similar_names():
    print 'Getting names'
    names = get_complete_names()
    print 'Total names:', len(names)
    # min similarity ratio between strings
    threshold_ratio = 0.8
    repeated = []    
    count = 0.0
    
    for i, str_1 in enumerate(names):
        for j in range(i+1, len(names)): 
            if count%10000 == 0:
                print 'Similar', count
            count+=1
            str_2 = names[j]
            
            if (difflib.SequenceMatcher(None, str_1, str_2).ratio() > threshold_ratio):
                print 'Similar', str_1, str_2
                repeated.append((str_1, str_2))
    
    with open( "repeated.p", "wb" ) as outfile:
        result = pickle.dump(outfile)
            
    return repeated
            


            
    







####################DATA#############################


thesis_ids = load_thesis_ids()

descriptors = load_descriptors()

name_genders = load_genders()

university_locations = {
    u'SANTIAGO DE COMPOSTELA':u'Galicia',
    u'AUT\xd3NOMA DE BARCELONA':u'Cataluña',
    u'UNIVERSITAT DE VAL\xc8NCIA (ESTUDI GENERAL)':u'Valencia',
    u'COMPLUTENSE DE MADRID':u'Madrid',
    u'OVIEDO':u'Asturias',
    u'AUT\xd3NOMA DE MADRID':u'Madrid',
    u'PA\xcdS VASCO/EUSKAL HERRIKO UNIBERTSITATEA':u'País Vasco',
    u'GRANADA':u'Andalucía',
    u'NACIONAL DE EDUCACI\xd3N A DISTANCIA':u'Madrid',
    u'BURGOS':u'Castilla y León',
    u'NAVARRA':u'Navarra',
    u'ALICANTE':u'Valencia',
    u'ROVIRA I VIRGILI':u'Cataluña',
    u'POLIT\xc9CNICA DE VALENCIA' :u'Valencia',
    u'SEVILLA' :u'Andalucía',
    u'EXTREMADURA' :u'Extremadura',
    u'ZARAGOZA' :u'Aragon',
    u'POMPEU FABRA' :u'Cataluña',
    u'POLIT\xc9CNICA DE MADRID' :u'Madrid',
    u'M\xc1LAGA' :u'Andalucía',
    u'POLIT\xc9CNICA DE CATALUNYA' :u'Cataluña',
    u'MIGUEL HERN\xc1NDEZ DE ELCHE' :u'Valencia',
    u'RIOJA' :u'La Rioja',
    u'CARLOS III DE MADRID' :u'Madrid',
    u'GIRONA' :u'Cataluña',
    u'BARCELONA' :u'Cataluña',
    u'VIGO' :u'Galicia',
    u'SALAMANCA' :u'Castilla y León',
    u'MURCIA' :u'Murcia',
    u'P\xdaBLICA DE NAVARRA' :u'Navarra',
    u'VALLADOLID' :u'Castilla y León',
    u'PALMAS DE GRAN CANARIA' :u'Islas Canarias',
    u'ALMER\xcdA' :u'Extremadura',
    u'LA LAGUNA' :u'Islas Canarias',
    u'LLEIDA' :u'Cataluña',
    u'C\xd3RDOBA' :u'Andalucía',
    u'C\xc1DIZ' :u'Andalucía',
    u'ILLES BALEARS' :u'Islas Baleares',
    u'ABAT OLIBA CEU' :u'Cataluña',
    u'ALCAL\xc1' :u'Madrid',
    u'DEUSTO' :u'País Vasco',
    u'EUROPEA DE MADRID' :u'Madrid',
    u'CANTABRIA' :u'Cantabria',
    u'JA\xc9N' :u'Andalucía',
    u'PONTIFICIA DE SALAMANCA' :u'Castilla y León',
    u'REY JUAN CARLOS' :u'Madrid',
    u'LE\xd3N' :u'Castilla y León',
    u'RAM\xd3N LLULL' :u'Cataluña',
    u'POLIT\xc9CNICA DE CARTAGENA' :u'Andalucía',
    u'PONTIFICIA COMILLAS' :u'Madrid',
    u'CASTILLA-LA MANCHA' :u'Castilla La Mancha',
    u'JAUME I DE CASTELL\xd3N' :u'Valencia',
    u'CAT\xd3LICA DE VALENCIA SAN VICENTE M\xc1RTIR' :u'Valencia',
    u'A CORU\xd1A' :u'Galicia',
    u'PABLO DE OLAVIDE' :u'Andalucía',
    u'SAN PABLO-CEU' :u'Madrid',
    u'HUELVA' :u'Andalucía',
    u'CARDENAL HERRERA-CEU' :u'Valencia',
    u'OBERTA DE CATALUNYA' :u'Cataluña',
    u'CAT\xd3LICA SAN ANTONIO' :u'Murcia',
    u'INTERNACIONAL DE CATALUNYA' :u'Cataluña',
    u'ANTONIO DE NEBRIJA' :u'Madrid',
    u'MONDRAG\xd3N UNIBERTSITATEA' :u'País Vasco',
    u'FRANCISCO DE VITORIA' :u'Madrid',
    u'CAMILO JOS\xc9 CELA' :u'Madrid',
    u'IE UNIVERSITY' :u'Madrid',
    u'INTERNACIONAL MEN\xc9NDEZ PELAYO' :u'Madrid',
    u'VIC' :u'Cataluña',
    u'INTERNACIONAL DE VALENCIA' :u'Valencia',
    u'ALFONSO X EL SABIO' :u'Madrid',
    u'A DISTANCIA DE MADRID' :u'Madrid',
    u'CAT\xd3LICA SANTA TERESA DE JES\xdaS DE \xc1VILA' :u'Castilla y León',
    u'SAN JORGE' :u'Aragón',
    u'INTERNACIONAL DE ANDALUC\xcdA' :u'Andalucía',
    u'EUROPEA MIGUEL DE CERVANTES' :u'Castilla y León',
    u'INTERNACIONAL DE LA RIOJA' :u'La Rioja',
}

university_ids = {
    1: u'SANTIAGO DE COMPOSTELA', 
    2: u'AUT\xd3NOMA DE BARCELONA', 
    3: u'UNIVERSITAT DE VAL\xc8NCIA (ESTUDI GENERAL)', 
    4: u'COMPLUTENSE DE MADRID', 
    5: u'OVIEDO', 
    6: u'AUT\xd3NOMA DE MADRID', 
    7: u'PA\xcdS VASCO/EUSKAL HERRIKO UNIBERTSITATEA', 
    8: u'GRANADA', 
    9: u'NACIONAL DE EDUCACI\xd3N A DISTANCIA', 
    10: u'BURGOS', 
    11: u'NAVARRA', 
    12: u'ALICANTE', 
    13: u'ROVIRA I VIRGILI', 
    14: u'POLIT\xc9CNICA DE VALENCIA', 
    15: u'SEVILLA', 
    16: u'EXTREMADURA', 
    17: u'ZARAGOZA', 
    18: u'POMPEU FABRA', 
    19: u'POLIT\xc9CNICA DE MADRID', 
    20: u'M\xc1LAGA', 
    21: u'POLIT\xc9CNICA DE CATALUNYA', 
    22: u'MIGUEL HERN\xc1NDEZ DE ELCHE', 
    23: u'RIOJA', 
    24: u'CARLOS III DE MADRID', 
    25: u'GIRONA', 
    26: u'BARCELONA', 
    27: u'VIGO', 
    28: u'SALAMANCA', 
    29: u'MURCIA', 
    30: u'P\xdaBLICA DE NAVARRA', 
    31: u'VALLADOLID', 
    32: u'PALMAS DE GRAN CANARIA', 
    33: u'ALMER\xcdA', 
    34: u'LA LAGUNA', 
    35: u'LLEIDA', 
    36: u'C\xd3RDOBA', 
    37: u'C\xc1DIZ', 
    38: u'ILLES BALEARS', 
    39: u'ABAT OLIBA CEU', 
    40: u'ALCAL\xc1', 
    41: u'DEUSTO', 
    42: u'EUROPEA DE MADRID', 
    43: u'CANTABRIA', 
    44: u'JA\xc9N', 
    45: u'PONTIFICIA DE SALAMANCA', 
    46: u'REY JUAN CARLOS', 
    47: u'LE\xd3N', 
    48: u'RAM\xd3N LLULL', 
    49: u'POLIT\xc9CNICA DE CARTAGENA', 
    50: u'PONTIFICIA COMILLAS', 
    51: u'CASTILLA-LA MANCHA', 
    52: u'JAUME I DE CASTELL\xd3N', 
    53: u'CAT\xd3LICA DE VALENCIA SAN VICENTE M\xc1RTIR', 
    54: u'A CORU\xd1A', 
    55: u'PABLO DE OLAVIDE', 
    56: u'SAN PABLO-CEU', 
    57: u'HUELVA', 
    58: u'CARDENAL HERRERA-CEU', 
    59: u'OBERTA DE CATALUNYA', 
    60: u'CAT\xd3LICA SAN ANTONIO', 
    61: u'INTERNACIONAL DE CATALUNYA', 
    62: u'ANTONIO DE NEBRIJA', 
    63: u'MONDRAG\xd3N UNIBERTSITATEA', 
    64: u'FRANCISCO DE VITORIA', 
    65: u'CAMILO JOS\xc9 CELA', 
    66: u'IE UNIVERSITY', 
    67: u'INTERNACIONAL MEN\xc9NDEZ PELAYO', 
    68: u'VIC', 
    69: u'INTERNACIONAL DE VALENCIA', 
    70: u'ALFONSO X EL SABIO', 
    71: u'A DISTANCIA DE MADRID', 
    72: u'CAT\xd3LICA SANTA TERESA DE JES\xdaS DE \xc1VILA', 
    73: u'SAN JORGE', 
    74: u'INTERNACIONAL DE ANDALUC\xcdA', 
    75: u'EUROPEA MIGUEL DE CERVANTES', 
    76: u'INTERNACIONAL DE LA RIOJA'
}




#this should be done the first time running this scripts    
if __name__=='__main__':
    print check_similar_names()
    print 'done'
    


