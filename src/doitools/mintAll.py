'''
Created on 3 june 2021

@author: ostlerr
'''
import database
import sys
import pyodbc
import json
from datetime import date
import configparser
import datacite
from dataCiteConnect import getDataCiteClient 
from datacite.schema41 import contributors, creators, descriptions, dates, sizes,\
    geolocations, fundingreferences, related_identifiers
from datacite import schema41



def getmdIDs():
    mdids = [];
    cur = database.getCursor()
    sql = 'select m.md_id, m.isReady, m.doi_created from metadata_document m where (m.doi_created is null and m.isReady = 2) and (isExternal is null or isExternal = 0)'
    #sql = '''SELECT distinct  md_id FROM related_identifiers where related_identifier like '%www.era.rothamsted.ac.uk%' order by md_id'''
    cur.execute(sql)
    results = cur.fetchall()  
    counter = 0  
    for row in results:         
        counter +=1  
        mdids.append(row.md_id)                   
    print (str(counter) + ' DOIs to mint')
    return mdids


if __name__ == '__main__':

    ids = getmdIDs();
    
    
    for item in ids:
        try:
            print(item)
            
            documentInfo = database.DocumentInfo()        
            documentInfo.mdId = item
            documentInfo = database.process(documentInfo)
                
            xname = "D:/doi_out/"+ str(item) + ".xml"
            fxname = open(xname,'w+')
            fxname.write(schema41.tostring(documentInfo.data))
            fxname.close()
            d = getDataCiteClient()
            d.metadata_post(schema41.tostring(documentInfo.data))
            doi = documentInfo.data['identifier']['identifier']
            d.doi_post(doi, documentInfo.url)
            database.logDoiMinted(documentInfo)
                
            print ("update metadata_document set doi_created = getdate() where md_id ="+str(item))
            print ("xml file saved in " + xname)
            print('done')
        except datacite.errors.DataCiteServerError as error:
            print(error)
        except:
            print("Unexpected error:", sys.exc_info()[0])   
            
        
    
