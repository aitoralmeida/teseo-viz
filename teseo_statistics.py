# -*- coding: utf-8 -*-
"""
Created on Tue Feb 18 11:20:55 2014

@author: aitor
"""

import mysql.connector
from cache import university_locations, university_ids, thesis_ids, descriptors, name_genders
import networkx as nx
import sys
import requests

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
        
def get_university_ids():      
    cnx = mysql.connector.connect(**config)
    cursor_unis = cnx.cursor()
    cursor_unis.execute("SELECT id, name FROM university")
    result = {}
    for university in cursor_unis:
        result[university[0]] = university[1]
    cursor_unis.close()
    return result

def get_number_phd_by_universities():
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    result = {}
    for key in university_ids.keys():
        print key
        uni_name = university_ids[key]
        print uni_name
        query = 'SELECT COUNT(*) FROM thesis WHERE university_id=' + str(key)
        print query
        cursor.execute(query)
        for count in cursor:
            result[uni_name] = count[0]
    cursor.close()
    
    return result
    
def build_panel_relations():   
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    G = nx.Graph()
    for cont, thesis in enumerate(thesis_ids):
        if cont%500 == 0:
            print 'creating network: ' + str(float(cont)/len(thesis_ids) * 100)
        query = 'SELECT person_id FROM panel_member WHERE thesis_id =' + str(thesis)
        cursor.execute(query)
        panel = []
        for person in cursor:
            panel.append(person[0])
        for i, person in enumerate(panel):
            source = person
            for j in range(i+1, len(panel)):
                target = panel[j]
                if G.has_edge(source, target):
                    G.edge[source][target]['weight'] += 1
                else:
                    G.add_edge(source, target, weight = 1)
    cursor.close()
    print 'Graph created'
    print '-Nodes:',len(G.nodes())
    print '-Edges:',len(G.edges())
    print 'Writing file'
    nx.write_gexf(G, 'panel_relations.gexf')
    
#the graph is too big. Nodes with not enough degree are deleted
#If a node has a degree of 4 or less it only has been in the panel of 1 viva
def filter_panel_relations(G, MIN_DEGREE = 5):   
    print 'Starting graph'
    print '-Nodes:',len(G.nodes())
    print '-Edges:',len(G.edges())
    degrees = G.degree()
    for d in degrees:
        if degrees[d] < MIN_DEGREE:
            G.remove_node(d)
    print 'Filtered graph'
    print '-Nodes:',len(G.nodes())
    print '-Edges:',len(G.edges())
            
  
    
def create_university_temporal_evolution_by_year():    
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    query = 'SELECT university_id, defense_date from thesis'
    cursor.execute(query)
    results = {2000:{u'DEUSTO':0}}
    for i, thesis in enumerate(cursor):
        print 'temporal', i            
        
        try:
            university = university_ids[thesis[0]]
            year = thesis[1].year
            if year in results.keys():
                unis = results[year]
                if university in unis.keys():
                    unis[university] += 1
                else:
                    unis[university] = 1            
            else:
                results[year] = {university:1}
         except AttributeError:
            print 'The thesis has no year in the database'
    cursor.close()
    return results
    
def create_region_temporal_evolution_by_year():
    by_university = create_university_temporal_evolution_by_year()
    by_region = {}
    for year in by_university:
        universities = by_university[year]
        for uni in universities:
            region = university_locations[uni]
            total_uni = universities[uni]               
            if year in by_region.keys():
                regions = by_region[year]
                if region in regions.keys():
                    regions[region] += total_uni
                else:
                    regions[region] = total_uni            
            else:
                by_region[year] = {region:total_uni}
    
    return by_region
    
def create_area_temporal_evolution_by_year():
    cnx = mysql.connector.connect(**config)
    cnx2 = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    query = 'SELECT id, defense_date from thesis'
    cursor.execute(query)
    results = {2000:{u'DEUSTO':0}}
    for i, thesis in enumerate(cursor):         
        try:
            thesis_id = thesis[0]
            year = thesis[1].year
            print 'Processing', thesis_id, ', year', year
            
            #get descriptors   
            cursor_desc = cnx2.cursor()
            query_desc = 'SELECT descriptor_id FROM association_thesis_description WHERE thesis_id=' + str(thesis_id)      
            cursor_desc.execute(query_desc)
            
            used_descriptors = []
            for desc in cursor_desc:
                used_descriptors.append(desc[0])          
            cursor_desc.close()
            
            
            if year in results.keys():
                descs = results[year]
                for descriptor_id in used_descriptors:
                    decriptor_text = descriptors[descriptor_id]
                    if decriptor_text in descs.keys():
                        descs[decriptor_text] += 1
                    else:
                        descs[decriptor_text] = 1            
            else:
                descs = {}
                for descriptor_id in used_descriptors:
                    decriptor_text = descriptors[descriptor_id]
                    descs[decriptor_text] = 1                
                results[year] = descs
        except AttributeError:
            print 'The thesis has no year in the database'
        except mysql.connector.errors.InternalError as ie:
            print 'Mysql error', ie.msg
    cursor.close()
    return results
    
    
    #http://api.genderize.io/?name=aitor
    

    
    
    
    
    
#def create_temporal_evolution_by_uni():
#    cnx = mysql.connector.connect(**config)
#    cursor = cnx.cursor()
#    query = 'SELECT university_id, defense_date from thesis'
#    cursor.execute(query)
#    results = {2000:{u'DEUSTO':0}}
#    for i, thesis in enumerate(cursor):
#        print 'temporal', i            
#        try:
#            university = university_ids[thesis[0]]
#            year = thesis[1].year
#            if year in results.keys():
#                unis = results[year]
#                if university in unis.keys():
#                    unis[university] += 1
#                else:
#                    unis[university] = 1            
#            else:
#                results[year] = {university:1}
#        except:
#            print 'undefined date or university'
#    cursor.close()
#    return results
#    
    

        
    
    
if __name__=='__main__':
    print 'start'
    print create_area_temporal_evolution_by_year()
    print 'done'

