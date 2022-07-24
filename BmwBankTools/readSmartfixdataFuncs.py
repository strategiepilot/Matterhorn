"""
Funktionsbibliothek zum Auslesen der Smartfix Archiv-Daten aus GAA 

Autor:          Andreas Barth
Version:        4.9.2020
Plattform:      HSDAP
Input Daten:    Smartfix Archivdaten, ca. 12k XML Files in Ordnerstrukturen nach Batch u. Document Id
Output:         Dataframe mit allen relevanten Informationen pro Email (als pkl-File persistiert)

"""

# Imports
# ==================  GENERAL PACKAGES ==============================
import pandas as pd
import numpy as np
import datetime as dt
from dateutil import parser as date_parser
import os, re, string, sys, warnings
import matplotlib
from matplotlib import pyplot as plt

# ==================  SPACY =========================================
import spacy
from spacy.tokenizer import Tokenizer
from spacy import displacy
from spacy.lang.de import German
# Deutsches LM mit NER u. POS
LMpath = '/home/q506010/0_LM/DE230MD/de_core_news_md/de_core_news_md-2.3.0'
nlp = spacy.load(LMpath)
tokenizer = Tokenizer(nlp.vocab)

# ==================  Reading XML Files ==============================
import xml.etree.ElementTree as et




# ********************************************************************************
# File Liste zum Auslesen generieren
def generateFileList(path, searchfile="Export.xml"):
    '''
    Durchsucht ein Startverzeichnis mit allen Unterverzeichnissen
    und erstellt eine Liste mit Dateipfaden für für jedes gefundene spezifische Searchfile (z.B. "Export.xml")
    
    Input:
        path: Startverzeichnis. Ab hier wird gesucht (inkl. aller Unterverzeicnissee)
        searchfile: Dateibezeichnung nach der gesucht wird (funktioniert auch nur mit einzelnen Strings z.B. "Exp")
    
    Output:
        Liste, jedes Element ist ein Dateipfad, der einer anderen Funktion übergeben werden kann
    '''
    
    fileList = [os.path.join(dirpath, searchfile) for dirpath, dirname, filename in os.walk(path) if searchfile in filename]
    print(f"Processed {len(fileList)} directories")
    return fileList



# ********************************************************************************
# Clean Text using Named Entity Recognition / Spacy
# === 3a. Auslesen der XML-Info mit Parserfunktion auf der jeweiligen XML-Datei === 
# === Liest nur die Dokumente aus, die EMAIL-BODY sind ===
def parseXMLfile(xmlfile):
    '''
    Liest alle use-case relevanten Informationen eines XML-Files in einen DF
    
    Input:
        xmlfile: XML File, das ausgelesen wird
    
    Output:
        Liste, jedes Element ist ein Dateipfad, der einer anderen Funktion übergeben werden kann
    '''
    
    # 1. Erstelle Positiv-Liste mit allen Dokumenten, die "isBODY" sind
    # 2. Lese die Email-Bodies und -infos aus denjenigen Dokumenten, die auf der Positiv-Liste sind
    
    tree = et.parse(xmlfile)
    root = tree.getroot()
    stackID = root.attrib["StackID"]
    
    # ein collector für jede benötigte Info aus dem XML-File
    stackID_collector        = []
    procID_collector         = []
    docID_collector          = []
    dokuType_collector       = []
    alteredBy_collector      = []
    body_collector           = []
    subject_collector        = []
    eingangskanal_collector  = []
    from_collector           = []
    dateTime_collector       = []
        
    # 1. Erstelle Positiv-Liste mit allen Dokumenten, die isBODY sind:
    set_docID_isBody = set()
    docs = root.findall("./PROCESS/DOCUMENT")   # Auslesen aller Objecte, die Typ "DOCUMENT" sind
    for doc in docs:
        docID = doc.get('DocID')                # Auslesen DocID
        # fieldfilter selektiert die Tags "FIELD", die als Attribut "imp_EDOC-isBody" mit "Value" 1 haben
        fieldfilter = [field for field in doc.findall(".//FIELD") if (field.attrib["Name"]=="imp_EDOC-isBody") and (field.attrib["Value"]=="1")] 
        if fieldfilter:                         # True wenn fieldfilter ein isBody-Attribut mit 1 gefunden hat
            set_docID_isBody.add(docID)         # DocID wird der Positiv-Liste (set_docID) aller Body-Docs hinzugefügt ()     

    # 2. Lese die Email-Bodies und -infos aus denjenigen Dokumenten, die auf der Positiv-Liste sind
    for proc in root.iter('PROCESS'):           # Iteriere über alle PROCESS Abschnitte
        procID = proc.get("ProcessID")

        # check if DocIds are in positive List docID_isBody
        docListe = [doc for doc in proc.findall("DOCUMENT") if doc.get('DocID') in set_docID_isBody]
        if docListe:
            for doc in docListe:               # Ausleseroutine aller doc-spezifischen Infos der Email
                
                # Stack-ID auslesen
                stackID_collector.append(stackID)
                
                # Process-ID auslesen
                procID_collector.append(procID)
                
                # Document-ID auslesen
                docID = doc.get('DocID')
                docID_collector.append(docID)
                
                # Document Type auslesen
                dokuType = doc.attrib["ExportName"]
                dokuType_collector.append(dokuType)
                
                # Altered By Information auslesen (Bearbeiter bei GAA)
                alteredBy = doc.attrib["AlteredBy"] 
                alteredBy_collector.append(alteredBy)
                
                # Emailbody auslesen
                fieldBody = [f for f in doc.findall(".//FIELD") if f.attrib["Name"]=="imp_EDOC-Body"][0]
                body = fieldBody.get("Value")
                body_collector.append(body)
                
                # Email Betreff auslesen
                fieldSubject = [f for f in doc.findall(".//FIELD") if f.attrib["Name"]=="imp_EDOC-Subject"][0]
                subject = fieldSubject.get("Value")
                subject_collector.append(subject)
                
                # Eingangskanal auslesen (redunddant ... ist immer E-MAIl)
                fieldEingangskanal = [f for f in doc.findall(".//FIELD") if f.attrib["Name"]=="imp_Eingangskanal"][0]
                eingangskanal = fieldEingangskanal.get("Value")
                eingangskanal_collector.append(eingangskanal)
                
                # Email Absender auslesen
                fieldFrom = [f for f in doc.findall(".//FIELD") if f.attrib["Name"]=="imp_EDOC-From"][0]
                from_ = fieldFrom.get("Value")
                from_collector.append(from_)
                
                # Datum & Uhrzeit der Email auslesen
                fieldDateTime = [f for f in doc.findall(".//FIELD") if f.attrib["Name"]=="imp_EDOC-Received"][0]
                dateTime = fieldDateTime.get("Value")
                dateTime_collector.append(dateTime)
                
                collects = [eingangskanal_collector, stackID_collector, procID_collector, docID_collector, dokuType_collector,
                            alteredBy_collector, dateTime_collector, from_collector, subject_collector, body_collector]
                
                return pd.concat([pd.Series(c) for c in collects], axis=1)
