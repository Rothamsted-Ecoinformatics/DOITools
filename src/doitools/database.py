'''
Created on 9 Aug 2018

@author: ostlerr
'''
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

class DocumentInfo:
    
    def __init__(self):
        self.url = None
        self.mdId = None
        self.data = None
        
        
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
        if hasattr(row,'type_value'):
            self.contributorType = row.type_value
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
#        
    def asCreatorJson(self):
        creator = dict(creatorName = self.fullname,givenName = self.givenName,familyName = self.familyName)
        if not self.nameIdentifiers is None:                
            creator["nameIdentifiers"] = self.nameIdentifiers
        creator["affiliations"] = self.affiliations
        return creator
    
    def asContributorJson(self):
        contributor = dict(contributorType = self.contributorType, contributorName = self.fullname, givenName = self.givenName, familyName = self.familyName)
        if not self.nameIdentifiers is None:                
            contributor["nameIdentifiers"] = self.nameIdentifiers
        contributor["affiliations"] = self.affiliations
        return contributor

def connect():
    config = configparser.ConfigParser()
    config.read('config.ini')
    dsn=config['SQL_SERVER']['DSN']
    uid = config['SQL_SERVER']['UID']
    pwd = config['SQL_SERVER']['PWD']
    con = pyodbc.connect('DSN='+dsn+';uid='+uid+';pwd='+pwd)
    #con = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=Z:\website development\datacite\DataCite Metadata database.accdb;')
    #con = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=D:\code\access\DataCite Metadata database.accdb;')
    return con

def getCursor():
    con = connect()
    cur = con.cursor()
    return cur

def getDocumentMetadata(mdId):
    cur = getCursor()
    cur.execute("""select m.md_id, m.url, m.identifier, m.identifier_type, m.title, p.organisation_name as publisher, publication_year, grt.type_value as grt_value, srt.type_value as srt_value,
        m.language, m.version, f.mime_type, f.extension, m.rights_text, m.rights_licence_uri, m.rights_licence, m.description_abstract,m.description_methods,m.description_toc,m.description_technical_info,m.description_quality,m.description_provenance,m.description_other,
        fl.fieldname, fl.geo_point_latitude, fl.geo_point_longitude
        from (((((metadata_document m
        inner join organisation p on m.publisher = p.organisation_id)
        inner join general_resource_types grt on m.grt_id = grt.grt_id)
        left outer join specific_resource_types srt on m.srt_id = srt.srt_id)
        inner join formats f on m.format_id = f.format_id)
        inner join experiment lte on m.lte_id = lte.experiment_id)
        inner join fields fl on lte.field_id = fl.field_id
        where m.md_id = ?""", mdId)
    return cur

def prepareCreators(mdId):
    cur = getCursor()
    creators = []
    # First prepare named people
    cur.execute('select p.family_name, p.given_name, p.name_identifier, p.name_identifier_scheme, p.scheme_uri, o.organisation_name, o.street_address, o.address_locality, o.address_region, o.address_country, o.postal_code from (person p inner join person_creator pc on p.person_id = pc.person_id) inner join organisation o on p.affiliation = o.organisation_id where pc.md_id = ?', mdId)
    
    results = cur.fetchall()    
    for row in results: 
        person = Person(row)        
        creators.append(person.asCreatorJson())
           
    # second prepare organisations
    cur.execute('select organisation_name from organisation o inner join organisation_creator oc on o.organisation_id = oc.organisation_id where oc.md_id = ?',mdId)
    results = cur.fetchall()
    for row in results:
        creators.append({"creatorName": row.organisation_name}) 
        
    return creators

def prepareContributors(mdId):
    cur = getCursor()
    contributors = [] 
    # First prepare named people
    cur.execute("""select p.family_name, p.given_name, p.name_identifier, p.name_identifier_scheme, p.scheme_uri, o.organisation_name, o.street_address, o.address_locality, o.address_region, o.address_country, o.postal_code, prt.type_value 
        from ((person p 
        inner join organisation o on p.affiliation = o.organisation_id) 
        inner join person_role pr on p.person_id = pr.person_id)
        inner join person_role_types prt on pr.prt_id = prt.prt_id
        where pr.md_id = ?""", mdId)
    
    results = cur.fetchall()    
    for row in results: 
        person = Person(row)        
        contributors.append(person.asContributorJson())
    # second prepare organisations
    cur.execute("""select o.organisation_name, ort.type_value 
        from (organisation o 
        inner join organisation_role r on o.organisation_id = r.organisation_id) 
        inner join organisation_role_types ort on r.ort_id = ort.ort_id
        where r.md_id = ?""",mdId)
    results = cur.fetchall()
    for row in results:
        contributors.append({"contributorName": row.organisation_name}) 
        
    return contributors    
    
def prepareSubjects(mdId):
    cur = getCursor()
    subjects = []
    cur.execute("""select s.subject, s.subject_uri, ss.subject_schema, ss.schema_uri
        from (subjects s
        inner join subject_schemas ss on s.ss_id = ss.ss_id)
        inner join document_subjects ds on s.subject_id = ds.subject_id 
        where ds.md_id = ?""", mdId)
    1535448515
    results = cur.fetchall()    
    for row in results: 
        subjects.append({'lang' : 'en', 'subjectScheme' : row.subject_schema, 'schemeURI' : row.schema_uri, 'valueURI' : row.subject_uri, 'subject' : row.subject})
        
    return subjects
    
def prepareDescriptions(row):
    descriptions = []
    
    descriptions.append({'lang' : row.language, 'descriptionType' : 'Abstract', 'description' : row.description_abstract})
    if not row.description_methods is None:
        descriptions.append({'lang' : row.language, 'descriptionType' : 'Methods', 'description' : row.description_methods})
    if not row.description_toc is None:
        descriptions.append({'lang' : row.language, 'descriptionType' : 'TableOfContents', 'description' : row.description_toc})
    if not row.description_technical_info is None:
        descriptions.append({'lang' : row.language, 'descriptionType' : 'TechnicalInfo', 'description' : row.description_technical_info})
    if not row.description_quality is None or not row.description_provenance is None or not row.description_other is None:
        descriptions.append({'lang' : row.language, 'descriptionType' : 'Other', 'description' : str(row.description_provenance) + " " + str(row.description_quality) + " " + str(row.description_other)})
    
    return descriptions

def prepareDates(mdId):
    cur = getCursor()
    dates = []
    cur.execute("""select dt.type_value, dd.document_date from document_dates dd inner join date_types dt on dd.dt_id = dt.dt_id where dd.md_id = ?""", mdId)
    
    results = cur.fetchall()    
    for row in results: 
        dates.append({'date': row.document_date.strftime('%Y-%m-%d'),'dateType' : row.type_value})
        
    return dates
    
def prepareRelatedIdentifiers(mdId):
    cur = getCursor()
    related_identifiers = []
    cur.execute("""select ri.related_identifier, i.type_value as identifier_type, r.type_value as relation_type
        from (related_identifiers ri
        inner join identifier_types i on ri.it_id = i.it_id)
        inner join relation_types r on ri.rt_id = r.rt_id
        where ri.md_id = ?""", mdId)
    
    results = cur.fetchall()    
    for row in results: 
        related_identifiers.append({'relatedIdentifier': row.related_identifier,'relatedIdentifierType' : row.identifier_type, 'relationType' : row.relation_type})
        
    return related_identifiers    

def prepareSizes(mdId):
    cur = getCursor()
    sizes = []
    cur.execute("""select u.unit_short_name, ds.size_value
        from document_sizes ds inner join measurement_unit u on ds.unit_id = u.unit_id where isIllustration = 0 and ds.md_id = ?""", mdId)
    
    results = cur.fetchall()    
    for row in results: 
        if row.unit_short_name == 'None':
            sizes.append(row.size_value)
        else:
            sizes.append(str(row.size_value) + ' ' + row.unit_short_name)
        
    return sizes

def prepareFundingReferences(mdId):
    cur = getCursor()
    fundingreferences = []
    cur.execute("""select fa.award_number, fa.award_uri, fa.award_title,fb.organisation_name, fb.funder_identifier, fb.funder_identifier_type
        from (document_funding df
        inner join funding_awards fa on df.fa_id = fa.fa_id)
        inner join organisation fb on fa.organisation_id = fb.organisation_id
        where df.md_id = ?""", mdId)

    results = cur.fetchall()
    for row in results:
        fundingreferences.append(
        {
            "funderName": row.organisation_name,
            "funderIdentifier": {
                "funderIdentifier": row.funder_identifier,
                "funderIdentifierType": row.funder_identifier_type
            },
            "awardNumber": {
                "awardNumber": row.award_number,
                "awardURI": row.award_uri
            },
            "awardTitle": row.award_title
        })
        
    return fundingreferences

def process(documentInfo):
    mdId = documentInfo.mdId
    mdCursor = getDocumentMetadata(mdId)
    mdRow = mdCursor.fetchone()
    data = None
    print("Document ID is: " + mdId)
    if mdRow:
        mdUrl = mdRow.url
        documentInfo.url = mdUrl
        print(documentInfo.url)
        data = {
            'identifier' : {
                'identifier' : mdRow.identifier,
                'identifierType' : 'DOI'
            },
            'creators' : prepareCreators(mdId),
            'titles' : [
                {'title' : mdRow.title}
            ],
            'publisher' : mdRow.publisher,
            'publicationYear' : mdRow.publication_year,
            'resourceType': {'resourceTypeGeneral' : mdRow.grt_value},
            'subjects' : prepareSubjects(mdId),
            'contributors' : prepareContributors(mdId),
            'dates' : prepareDates(mdId),
            'language' : mdRow.language,        
            'version' : str(mdRow.version),
            'relatedIdentifiers' : prepareRelatedIdentifiers(mdId),
            'sizes' : prepareSizes(mdId),
            'formats' : [mdRow.mime_type],
            'rightsList' : [
                {'rightsURI' : mdRow.rights_licence_uri, 'rights' : mdRow.rights_licence},
                {'rights' : mdRow.rights_text}
            ],
            'descriptions' : prepareDescriptions(mdRow),
            'geoLocations': [
                {
                    'geoLocationPoint' : {
                        'pointLongitude': float(mdRow.geo_point_longitude),
                        'pointLatitude': float(mdRow.geo_point_latitude)
                    },
                    'geoLocationPlace': mdRow.fieldname            
                }
            ],
            'fundingReferences' : prepareFundingReferences(mdId)
        }
        
        
    documentInfo.data = data
    strJsData =  json.dumps(data, indent=4)
    print(strJsData)
    return documentInfo    

def logDoiMinted(documentInfo):
    try:
        con = connect()
        cur = con.cursor()
        cur.execute("update metadata_document set doi_created = getdate() where md_id = ?", documentInfo.mdId)
        con.commit()
    except AttributeError as error:
        print(error)
    except pyodbc.Error as error:
        print(error)
    

try:
    documentInfo = DocumentInfo()        
    documentInfo.mdId = input('Enter Document ID: ')
    documentInfo = process(documentInfo)
    
    xname = "D:/doi_out/"+ str(documentInfo.mdId) + ".xml"
    fxname = open(xname,'w+')
    fxname.write(schema41.tostring(documentInfo.data))
    fxname.close()
    d = getDataCiteClient()
    d.metadata_post(schema41.tostring(documentInfo.data))
    doi = documentInfo.data['identifier']['identifier']
    d.doi_post(doi, documentInfo.url)
    logDoiMinted(documentInfo)
    docID =  documentInfo.mdId
    print ("update metadata_document set doi_created = getdate() where md_id ="+docID)
    print ("xml file saved in " + xname)
    print('done')

except datacite.errors.DataCiteServerError as error:
    print(error)
except:
    print("Unexpected error:", sys.exc_info()[0])        
    

