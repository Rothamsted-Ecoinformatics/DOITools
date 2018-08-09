import pyodbc
import json
'''
Created on 9 Aug 2018

@author: ostlerr
'''
from datacite.schema40 import nameidentifiers

class Person:
    def __init__(self, row):
        self.familyName = row.family_name
        self.givenName = row.given_name 
        self.nameIdentifier = row.name_identifier 
        self.nameIdentifierScheme = row.name_identifier_scheme 
        self.schemeUri = row.scheme_uri 
        self.organisationName = row.organisation_name 
        self.street = row.street_address
        self.locality = row.address_locality 
        self.region = row.address_region 
        self.country = row.address_country 
        self.postalCode = row.postal_code
        self.fullname = self.givenName + " " + self.familyName
        
        self.nameIdentifiers = None
        if not self.nameIdentifier is None:
            self.nameIdentifiers = [
                {
                    "nameIdentifier": self.nameIdentifier,
                    "nameIdentifierScheme": self.nameIdentifierScheme,
                    "schemeURI": self.schemeUri 
                }
            ]
        
        self.affiliations = [self.formatAddress()]
        
    def formatAddress(self):
        address = self.organisationName
        if not self.street is None:
            address = address + ", " + self.street
        if not self.locality is None:
            address = address + ", " + self.locality
        if not self.region is None:
            address = address + ", " + self.region
        if not self.postalCode is None:
            address = address + ", " + self.postalCode
        if not self.country is None:
            address = address + ", " + self.country
        return address
    
#     def affiliationsAsJson(self):
#         affiliations = [self.formatAddress()]
#         return affiliations
    
#     def nameIdentifierAsJson(self):
#         
#         return nameIdentifiers
#        
    def personAsJson(self):
        creator = dict(creatorName = self.fullname,givenName = self.givenName,familyName = self.familyName)
        if not self.nameIdentifiers is None:                
            creator["nameIdentifiers"] = self.nameIdentifiers
        creator["affiliations"] = self.affiliations
        
        return json.dumps(creator)

def connect():
    con = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\ostlerr\OneDrive - Rothamsted Research\ERA\DataCite Schema database\DataCite Metadata database.accdb;')
    return con

def getCursor():
    con = connect()
    cur = con.cursor()
    return cur

def getDocumentMetadata():
    cur = getCursor()
    cur.execute('select * from metadata_document')
    return cur

def prepareCreators(mdId):
    cur = getCursor()
    creators = {}
    # First prepare named people
    cur.execute('select p.family_name, p.given_name, p.name_identifier, p.name_identifier_scheme, p.scheme_uri, o.organisation_name, o.street_address, o.address_locality, o.address_region, o.address_country, o.postal_code from (person p inner join person_creator pc on p.person_id = pc.person_id) inner join organisation o on p.affiliation = o.organisation_id where pc.md_id = ?', mdId)
    
    results = cur.fetchall()    
    for row in results: 
        person = Person(row)
        
         
        print (person.personAsJson())
        
        creators[person.fullname] = person.personAsJson()
#                 "creatorName": person.fullname,
#                 "givenName": person.givenName,
#                 "familyName": person.familyName,                
#                 "nameIdentifiers": person.nameIdentifierAsJson(),
#                 "affiliations": person.affiliationsAsJson()
            
    # second prepare organisations
    cur.execute('select organisation_name from organisation o inner join organisation_creator oc on o.organisation_id = oc.organisation_id where oc.md_id = ?',mdId)
    results = cur.fetchall()
    for row in results:
        creators.append({"creatorName": row.organisation_name}) 
        
    return creators
    

    
#def prepareContributors(mdId):
    
#def prepareSubjects(mdId):

def process():
    mdCursor = getDocumentMetadata()
    mdRow = mdCursor.fetchone()
    if mdRow:
        mdId = mdRow.md_id
        creators = [prepareCreators(mdId)]
        print(creators)
#        prepareContributors(mdId)
#         prepareSubjects(mdId)
#         data = {
#             'identifier' : {
#                 'identifier' : mdRow.identifier,
#                 'identifierType' : 'DOI'
#             },
#             'creators' : [
#                 {'creatorName' : 'Rothamsted Experimental Station'}
#             ],
#             'titles' : [
#                 {'title' : bookTitle}
#             ],
#             'publisher' : 'Lawes Agricultural Trust',
#             'publicationYear' : year,
#             'resourceType': {'resourceTypeGeneral' : 'Text'},
#             'subjects' : subjectsu,
#             'contributors' : [
#                 {'contributorType' : 'Distributor', 'contributorName' : 'Rothamsted Research'},
#                 {'contributorType' : 'HostingInstitution', 'contributorName' : 'Rothamsted Research'},
#                 {'contributorType' : 'RightsHolder', 'contributorName' : 'Lawes Agricultural Trust'},
#                 {'contributorType' : 'DataCurator', 'contributorName' : 'E-RA Curator Team'},
#                 {'contributorType' : 'ContactPerson', 'contributorName' : 'E-RA Curator Team'}
#             ],
#             'dates' : [
#                 {'date' : year, 'dateType' : 'Created'}
#             ],
#             'language' : 'en',        
#             'version' : '1.0',
#             'sizes' : [
#                 pages
#             ],
#             'formats' : [
#                 'appplication/PDF'
#             ],
#             'rightsList' : [
#                 {'rightsURI' : 'http://creativecommons.org/licenses/by/4.0', 'rights' : 'This work is licensed under a Creative Commons Attribution 4.0 International License'},
#                 {'rights' : '@Copyright Lawes Agricultural Trust Ltd'}
#             ],
#             'descriptions' : [
#                 {'lang' : 'en', 'descriptionType' : 'Abstract', 'description' : abstract}
#             ],
#             'geoLocations' : [
#                 {'geoLocationPlace' : 'Rothamsted Research, Harpenden, UK'}
#             ],
#             'fundingReferences' : [
#                 {'funderName' : 'Biotechnology and Biological Sciences Research Council', 
#                 'funderIdentifier' : {
#                     'funderIdentifier' : 'http://dx.doi.org/10.13039/501100000268', 'funderIdentifierType' : 'Crossref Funder ID'
#                 }},
#                 {'funderName' : 'Lawes Agricultural Trust'}
#             ]
#         }
        
process()
