"""
Funktionsbibliothek für Anonymisierung und Cleaning von Emails
Autor: Andreas Barth

Functions:
    CleanNER():        Anonymisierung von Emailtexten (Bodies, Header) mit Spacy NER Funktionalität
    CleanPLZ():        Lookup-Funktion auf Postleitzahlen-Dictionary
    CleanRGX():        Anonymisierung von Emailtexten (Bodies, Header) mit Regex-Regeln
    CleanREPLACE(df):  Bereinigung der Emailtextse (ersetzen von Prasen, etc.)
    CleanREPLACE_KOFAX_Export(df):  Bereinigung der Emailtextse (ersetzen von Prasen, etc.)

"""

# Imports
# ==================  GENERAL PACKAGES ==============================
import pandas as pd
import numpy as np
import datetime as dt
import os, re, string, sys, warnings
from IPython.core.interactiveshell import InteractiveShell
import matplotlib
from matplotlib import pyplot as plt
# ==================  SPACY =========================================
import spacy
from spacy.tokenizer import Tokenizer
from spacy import displacy
from spacy.lang.de import German
LMpath = '/home/q506010/0_LM/Spacy/DE230MD/de_core_news_md/de_core_news_md-2.3.0' # Deutsches LM mit NER u. POS
nlp = spacy.load(LMpath)
tokenizer = Tokenizer(nlp.vocab)
print("Spacy Language Model German md-2.3.0 instantiated in nlp()")









# ********************************************************************************
# Clean Text using Named Entity Recognition / Spacy
def CleanNER(txt):
    '''    
    Prüft übergebene Texte mit Named-Entity-Recognition Funktion von Spacy.
    Die NER-Funktion von Spacy benötigt das Language Model "de_core_news_md-2.3.0" oder höher ( wird mit nlp. Methode getriggert)
    Funktion wird auf einem Dataframe z.B. durch eine Lambda-Funktion aufgerufen.
    
    Input:   Text / String
    Output:  Test / String mit bereinigten NER-Tags:
        <PER> = Person, <ORG> = Organisation, <LOC> für Location
    '''
    doc = nlp(txt)
    cleanString = txt
    for e in reversed(doc.ents): #reversed to not modify the offsets of other entities when substituting
        start = e.start_char
        end = start + len(e.text)
        if e.label_ not in ["MISC"]:  # Entity-Label MISC wird ausgenommen, da zu ungenau
            cleanString = cleanString[:start] + "<" + e.label_ + ">" + cleanString[end:]
    return cleanString



# ********************************************************************************
# Clean Text using Lookup-Verzeichnis für Postleitzahlen
def CleanPLZ(txt):
    '''    
    Prüft übergebene Texte mit einem Look-Up im Set PLZZAHLEN (wird aus Dataframe geladen)
    
    Input:   Text / String
    Output:  Test / String mit vertaggten Postleitzahlen <PLZ>
    '''
    
    path = '/home/q506010/2_LookupData/'
    plzDF = pd.read_pickle(path+"Plz_DF.pkl").dropna()
    PLZZAHLEN = {i.upper() for i in plzDF.tolist()}
    
    tokenizer = nlp.Defaults.create_tokenizer(nlp)
    toks = [t for t in tokenizer(txt)]
    clean_tokens = []
  
    for tok in toks:
        toktest = tok.text.upper().strip()       
        if toktest in PLZZAHLEN:
            tok = "<PLZ>"
            clean_tokens.append(tok)
        else:
            clean_tokens.append(tok)
                
    return " ".join([str(t) for t in clean_tokens])



# ********************************************************************************
# Clean Text using Regex Regeln => Anonymisieren v. personenbezogenen Daten in Emailtexten
def CleanRGX(txt):
    '''    
    Vertaggt personenbezogene Daten in übergebenen Texten. 
    Umfangreiches Regex-Regelwerk identifiziert bst. personenbez. Daten und ersetzt diese mit <TAG>
    
    Input:   Text / String
    Output:  Test / String mit vertaggten personenbezogenen Daten, <TAG-spezifisch>
    '''
    
    # Compilation - Regex Objekte
    RGX_Datum            = re.compile(r'\b(31|30|[012]\d|\d)\.(0\d|1[012]|\d)\.([12]{1}\d{1,3})\b|\b(31|30|[012]\d|\d)\.(0\d|1[012]|\d)\.([1-9]{1}\d)\b')
    RGX_Personalausweis  = re.compile(r'\b[CFGHJKLMNPRTVWXYZ]{1}[CFGHJKLMNPRTVWXYZ\d]{8}\b')
    RGX_SteuerNr         = re.compile(r'\w{2}/\d{3}/\d{4,5}\b')
    RGX_SteuerID         = re.compile(r'\b[1-9]{1}[0-9]{1}\s\d{3}\s\d{3}\s\d{3}\b|\b[1-9]{1}\d{10}\b')
    RGX_TelefonFax       = re.compile(r'(((((((00|\+)49[ \-/]?)|0)[1-9][0-9]{1,4})[ \-/]?)|((((00|\+)49\()|\(0)[1-9][0-9]{1,4}\)[ \-/]?))[0-9]{1,7}([ \-/]?[0-9]{1,5})?)\b')
    RGX_Email            = re.compile(r'\b[\w.+-]+@[\w-]+\.[a-zA-Z0-9-.]+\b')
    RGX_IBAN             = re.compile(r'\bDE\d{2} \d{4} \d{4} \d{4} \d{4} \d{2}\b|\bDE\d{20}\b')
    RGX_MRefLeasing      = re.compile(r'\bLC\d{4}X\d{8}\b')
    RGX_MRefCredit       = re.compile(r'\bCF\d{12}\b')
    RGX_VINlong          = re.compile(r'\b[A-Z]{1}[A-Z\d]{16}\b')
    RGX_VINshort         = re.compile(r'\b[A-Z]{1}[A-Z\d]{6}\b')
    RGX_KFZkennzeichen   = re.compile(r'\b[a-zA-ZöüäÖÜ]{1,3}[ -][a-zA-ZöüäÖÜ]{1,2} ?[1-9]{1}[0-9]{1,3}[^.]\b')
    RGX_Qnummer          = re.compile(r'\b[qQ][\-\s]?\d{5,6}\b')
    RGX_PLZ              = re.compile(r'\b([0]{1}[1-9]{1}|[1-9]{1}[0-9]{1})[0-9]{3}\b')
    RGX_6digits          = re.compile(r'\b\d{6}\b')
    RGX_7digits          = re.compile(r'\b\d{7}\b')
    RGX_8digits          = re.compile(r'\b\d{8}\b')
    RGX_9digits          = re.compile(r'\b\d{9}\b')
    RGX_10digits         = re.compile(r'\b\d{10}\b')
    RGX_URL              = re.compile(r'\b<*(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?>*\b')
    RGX_URL2             = re.compile(r'\b((https?|ftp|smtp):\/\/)?(www.)?[\w-]+\.[a-z]+(\/[a-zA-Z0-9#]+\/?)*\b')
    
    RGX_Intro            = re.compile( r'(Page 1.*)Betreff:', flags=re.DOTALL)
    
    # TAG Bezeichnungen für das Replacement
    TAG_Datum            = " <DATUM> "
    TAG_Personalausweis  = " <PASSPORT> "
    TAG_SteuerNr         = " <TAXNBR> "
    TAG_SteuerID         = " <TAXNBR> "
    TAG_TelefonFax       = " <TELFAX> "
    TAG_Email            = " <EMAIL> "
    TAG_IBAN             = " <IBAN> "
    TAG_MRefLeasing      = " <MREFLS> "
    TAG_MRefCredit       = " <MREFCR> "
    TAG_VINshort         = " <VIN> "
    TAG_VINlong          = " <VIN> "
    TAG_KFZkennzeichen   = " <KFZKZ> "
    TAG_Qnummer          = " <QNBR> "
    TAG_PLZ              = " <PLZ> "
    TAG_6digits          = " <666666> "
    TAG_7digits          = " <7777777> "
    TAG_8digits          = " <88888888> "
    TAG_9digits          = " <999999999> "
    TAG_10digits         = " <1010101010> "
    TAG_URL              = " <URL> "
   
    # Replace RGX-Ausdruck mit TAG
    try:
        emailClean = txt
        
        emailClean = RGX_Intro.sub("<INTRO> ", emailClean)
        
        emailClean = RGX_IBAN.sub(TAG_IBAN, emailClean)
        emailClean = RGX_MRefLeasing.sub(TAG_MRefLeasing, emailClean)
        emailClean = RGX_MRefCredit.sub(TAG_MRefCredit, emailClean)
        emailClean = RGX_Datum.sub(TAG_Datum, emailClean)
        emailClean = RGX_Personalausweis.sub(TAG_Personalausweis, emailClean)
        emailClean = RGX_SteuerNr.sub(TAG_SteuerNr, emailClean)
        emailClean = RGX_SteuerID.sub(TAG_SteuerID, emailClean)
        emailClean = RGX_TelefonFax.sub(TAG_TelefonFax, emailClean)
        emailClean = RGX_Email.sub(TAG_Email, emailClean)
        emailClean = RGX_MRefLeasing.sub(TAG_MRefLeasing, emailClean)
        emailClean = RGX_MRefCredit.sub(TAG_MRefCredit, emailClean)
        emailClean = RGX_VINlong.sub(TAG_VINlong, emailClean)
        emailClean = RGX_VINshort.sub(TAG_VINshort, emailClean)
        emailClean = RGX_KFZkennzeichen.sub(TAG_KFZkennzeichen, emailClean)
        emailClean = RGX_Qnummer.sub(TAG_Qnummer, emailClean)
        emailClean = RGX_PLZ.sub(TAG_PLZ, emailClean)
        emailClean = RGX_6digits.sub(TAG_6digits, emailClean)
        emailClean = RGX_7digits.sub(TAG_7digits, emailClean)
        emailClean = RGX_8digits.sub(TAG_8digits, emailClean)
        emailClean = RGX_9digits.sub(TAG_9digits, emailClean)
        emailClean = RGX_10digits.sub(TAG_10digits, emailClean)
        emailClean = RGX_URL.sub(TAG_URL, emailClean)
        emailClean = RGX_URL2.sub(TAG_URL, emailClean)
        
        
        
        
    except:
        emailClean = "ANONYMIZATION FAILED"

    return emailClean.strip()



# ********************************************************************************
# Clean Text using Replace und Regex Regeln => Anonymisieren v. personenbezogenen Daten in Emailtexten
def CleanREPLACE(df):
    '''    
    Bereinigt übergebene Texte im übergebenen Dataframe df. Ersetzt geläufige Phrasen ("Sehr geehrte ..., Mit freundlichen ...").
    Benutzt Replace-Formeln und Regex-Formeln
    
    Input:   df mit mit Spalte "SUBJECT" und "BODY"
    Output:  df mit bereinigten Texten in Spalte "SUBJECT" und "BODY_CLEAN"
    '''
    
    TAG_Anrede = "<Anrede>"
    TAG_MfG    = "<MFG>" 
    
    # Nicht druckbare oder irrelevante Zeichenfolgen entfernen
    df["SUBJECT"] = df.SUBJECT.str.replace("\r", " ", regex=False)
    df["SUBJECT"] = df.SUBJECT.str.replace("\t", " ", regex=False)
    df["SUBJECT"] = df.SUBJECT.str.replace("\n", " ", regex=False)
    df["SUBJECT"] = df.SUBJECT.str.replace("\xa0", " ", regex=False)
    df["SUBJECT"] = df.SUBJECT.str.replace(r'[-_=#]{3,}', " ", regex=True)
    
    df["BODY_CLEAN"] = df.BODY.str.replace("\r", " ", regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("\t", " ", regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("\n", " ", regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("\xa0", " ", regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(r'[-_=#]{3,}', " ", regex=True)
    
    # Anrede, akad. Titel
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Dr. ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Prof. ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Mag. ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Dipl.-Ing. ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Dipl.-Ing. (FH) ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Dipl.-Kfm. ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Dipl.-Kfm. (FH) ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Dipl.-Psychologin ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Ph.D. ", "")
    
    # Ende der Email trimmen, wenn BMW-Sender-Email anfängt, bzw. Endformeln / Trimming am Ende der Email 
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace( r'Am .* schrieb bmw\.bank@bmw\.de:.*', "<TRIM EOFT>", regex= True,)
    
    # Anrede / Grußformeln
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Sehr geehrte Damen und Herren", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Geehrte Damen und Herren", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Sehr geehrtes Team BMW-Bank", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Werte Damen und Herren", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Sehr geehrte Herr Martin", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Herr Martin", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Sehr geehrte Frau", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Sehr geehrte", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Sehr geehrter", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Guten Tag", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Guten Tag zusammen", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Guten Morgen", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Guten Abend", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Hallo zusammen", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Hallo Team BMW-Bank", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Hallo Kundenbetreuung", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Hallo!", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Hallo", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Dear Sir/Madam", TAG_Anrede, regex=False)
  
    # Grußformel Verabschiedung
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Herzliche Grüße", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Mit freundlichen Grüßen", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Mit besten Grüßen", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Beste Grüße", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Mit freundlichem Gruß", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Viele Grüße", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Best regards", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Regards", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("MfG", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("MFG", TAG_MfG, regex=False)

    # Irrelevante Standardklauseln u. -phrasen entfernen
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(r'[dD]iese Nachricht wurde .* gesendet', "<VERSENDET-MIT>", regex= True,)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(r'[gG]esendet mit .* [aA]pp', "<VERSENDET-MIT>", regex= True,)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(r'[vV]on .* gesendet', "<VERSENDET-MIT>", regex= True,)    
       
    return df




def CleanREPLACE_KOFAX_Export(df):
    '''    
    Bereinigt übergebene Texte im übergebenen Dataframe df. Ersetzt geläufige Phrasen ("Sehr geehrte ..., Mit freundlichen ...").
    Benutzt Replace-Formeln und Regex-Formeln
    
    Input:   df mit mit Spalte "SUBJECT" und "BODY"
    Output:  df mit bereinigten Texten in Spalte "SUBJECT" und "BODY_CLEAN"
    '''
    
    TAG_Anrede = "<Anrede>"
    TAG_MfG    = "<MFG>" 
    TAG_BANK   = "<BMWB>"
    
    # Nicht druckbare oder irrelevante Zeichenfolgen entfernen
#     df["SUBJECT"] = df.SUBJECT.str.replace("\r", " ", regex=False)
#     df["SUBJECT"] = df.SUBJECT.str.replace("\t", " ", regex=False)
#     df["SUBJECT"] = df.SUBJECT.str.replace("\n", " ", regex=False)
#     df["SUBJECT"] = df.SUBJECT.str.replace("\xa0", " ", regex=False)
#     df["SUBJECT"] = df.SUBJECT.str.replace(r'[-_=#]{3,}', " ", regex=True)

    df["BODY_CLEAN"] = df.RAWBODY.str.replace("\r", " ", regex=False)
    
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("\t", " ", regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("\n", " ", regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("\xa0", " ", regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(r'[-_=#]{3,}', " ", regex=True)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("\ufeff", "", regex=False)
    
    # Organsisationen
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("BMW Financial Services BMW Bank GmbH ", TAG_BANK)
    
    
    # Anrede, akad. Titel
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Dr. ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Prof. ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Mag. ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Dipl.-Ing. ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Dipl.-Ing. (FH) ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Dipl.-Kfm. ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Dipl.-Kfm. (FH) ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Dipl.-Psychologin ", "")
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(" Ph.D. ", "")
    
    # Ende der Email trimmen, wenn BMW-Sender-Email anfängt, bzw. Endformeln / Trimming am Ende der Email 
    #df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace( r'Am .* schrieb bmw\.bank@bmw\.de:.*', "<TRIM EOFT>", regex= True,)
    
    # Anrede / Grußformeln
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Sehr geehrte Damen und Herren", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Geehrte Damen und Herren", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Sehr geehrtes Team BMW-Bank", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Werte Damen und Herren", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Sehr geehrte Herr Martin", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Herr Martin", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Sehr geehrte Frau", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Sehr geehrte", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Sehr geehrter", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Guten Tag", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Guten Tag zusammen", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Guten Morgen", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Guten Abend", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Hallo zusammen", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Hallo Team BMW-Bank", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Hallo Kundenbetreuung", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Hallo!", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Hallo", TAG_Anrede, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Dear Sir/Madam", TAG_Anrede, regex=False)
  
    # Grußformel Verabschiedung
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Herzliche Grüße", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Mit freundlichen Grüßen", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Mit besten Grüßen", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Beste Grüße", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Mit freundlichem Gruß", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Viele Grüße", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Best regards", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("Regards", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("MfG", TAG_MfG, regex=False)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace("MFG", TAG_MfG, regex=False)

    # Irrelevante Standardklauseln u. -phrasen entfernen
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(r'[dD]iese Nachricht wurde .* gesendet', "<VERSENDET-MIT>", regex= True,)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(r'[gG]esendet mit .* [aA]pp', "<VERSENDET-MIT>", regex= True,)
    df["BODY_CLEAN"] = df.BODY_CLEAN.str.replace(r'[vV]on .* gesendet', "<VERSENDET-MIT>", regex= True,)    
       
    return df

