"""
Funktionsbibliothek für Email-Download
Autor: Andreas Barth

Functions:
    connect_IMAP()             Anmelden an der BMW Bank Emailadresse bmw.bank@bmw.de
    get_mail_ids()             Auslesen der Mail-IDs auf einem Postfach
    get_header()               Auslesen d. Header-Infos aus einem Email-Message-Objekt (imap)
    get_body()                 Auslesen des Email-Bodies aus einem Email-Message-Objekt (imap)
    getSpecificFiles():        Spez. Dateinamen von Emailanhängen extrahieren
    get_attachments():         Auslesen von Emailattachments u. ggf. abspeichern
    uniqueMessageID():         Erzeugt eine unique Message ID
    readEmailBatch():          Liest Email-Batch aus Postfach u. ruft die o.g. Funktionen, speichert die Emailinformatinen als Dataframe ab (*.pkl)
    detectEncodingTXT():       Ermittelt Encoding einer zugeführten Email
    def detectEncodingDF():    Ermittelt Encoding für einen kompletten Dataframe (Mailcontainer)
    decodeEmailBody():         Dekodiert den enkodierten Emailbody der Emails in einem Mailcontainer (Pandas Dataframe)
    saveMAILCONTAINER_2disk(): Speichert einen Pandas Dataframe (Mailcontainer) in 2facher Ausführung ab
    decodeEmailCorpus():       Dekodiert den enkodierten Emailbody der Emails in einem Mailcontainer (Pandas Dataframe) 
    loadBlacklist()            Aufruf der BLACKLIST Emailabsender (Set)
    checkBLacklist()           Abgleich einer Texteinheit (z.B. FROM Feld) mit Blacklist Emailabsender
    
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
LMpath = '/home/q506010/0_LM/Spacy/DE230MD/de_core_news_md/de_core_news_md-2.3.0'
nlp = spacy.load(LMpath)
tokenizer = Tokenizer(nlp.vocab)

# ==================  Downloading & Processing Emails================
import json
import imaplib
import email
from email.header import decode_header
import mimetypes
import chardet
from chardet.universaldetector import UniversalDetector
from dateutil import parser as date_parser









# ********************************************************************************
# connect_IMAP()
def connect_IMAP(id_mailbox = "mb_001", credfile = 'ab_imap_creds.json'):
    '''    
    Automatisches Anmelden an einer Imap-Emailinbox.
    Input:   Pfad mit den Log-On Credentials (json-File)
    Output:  Connection Objekt für weitere Abfragen an die Mailbox
    '''
    CREDFILE = credfile
    CREDPATH = '/home/q506010/0_Admin/'
    with open(os.path.join(CREDPATH, CREDFILE)) as f:
        creds = json.load(f)

    ID = id_mailbox
    SERVER, PORT = creds["host"], creds["port"]
    USER, PASSCODE = creds[ID]["user"], creds[ID]["pw"]
    
    # Create Connection Object
    con = imaplib.IMAP4_SSL(SERVER)
    con.login(USER, PASSCODE)
    print(f"Sucess: Mailbox {ID} via IMAP connected.")
    return con

def test(x):
    inside = "Andreas"
    return outside+inside+x

# ********************************************************************************
# get_mail_ids()
def get_mail_ids(con, filter='ALL'):
    '''
    Auslesen der Email-IDs eines vorab selektieren Postfaches 
    Input:   Connection Objekt, Filterdefinition (default="All")
    Output:  Liste mit Email-IDS: mail_ids
    ''' 
    status, data = con.search(None, filter)
    mail_ids = []
    for block in data:
        mail_ids += block.split()
    print(f"{len(mail_ids)} Mail IDs ausgelesen")
    return mail_ids


# ********************************************************************************
# get_header()
def get_header(msg):
    '''
    Extrahiert den Email-Header einer Email-Message (email-message Objekt).
    Input:   email-message Objekt
    Output:  Email-Header Dictionary
    '''
    header_dict = {}
    header_dict["to"] = msg["to"]
    header_dict["from"] = msg["from"]
    header_dict["cc"] = msg["cc"]
    header_dict["bcc"] = msg["bcc"]
    header_dict["sub"] = msg["subject"]
    header_dict["date_raw"] = msg["date"]
    header_dict["date"] =  date_parser.parse(header_dict['date_raw']).strftime ("%d-%m-%Y")
    header_dict["time"] =  date_parser.parse(header_dict['date_raw']).strftime ("%H:%M:%S")
    header_dict["ctype"] = msg["content-type"]
    header_dict["cte"] = msg["Content-Transfer-Encoding"] #Content-Transfer-Encoding: quoted-printable
    header_dict["umid"] = msg["Message-ID"] 
    return header_dict


# ********************************************************************************
# get_body()
def get_body(msg):
    '''
    Extrahiert den Email-Body einer Email-Message (email-message Objekt)
    Input:   Email-message Objekt
    Output:  Email-Body (String)
    '''
    if msg.is_multipart():
        raw =  get_body(msg.get_payload(0, decode=False))
    else:
        raw =  msg.get_payload(None,True)
    return raw


# ********************************************************************************
# getSpecificFiles()
def getSpecificFiles(listeFiles):
    '''
    Extrahiert Dateinamen für spezifische Dateianhänge (nicht automatisch generierte Anhänge)
    Input:   listFiles => Liste mit Dateinamen
    Output:  Liste mit spezifischen Dateinamen
    '''
    return [specfile for specfile in listeFiles if "msg-part" not in specfile]



# ********************************************************************************
# get_attachements()
def get_attachments(msg, save2disk=False):
    '''
    Liest die Attachments einer zugeführten Raw-Email-Message (byte-Codierung) aus und speichert diese ab
    Input:   email-message Objekt
    Output:  Wenn save2disk = True: wird jeder Anhang im <save_path_> Ordner abgespeichert
    '''
    
    import mimetypes

    attachments = []
    counter = 1
    for part in msg.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        
        filename = part.get_filename()
        content_type = part.get_content_type()
        
        if not filename:
            ext = mimetypes.guess_extension(part.get_content_type())
            if 'text' in content_type:
                ext = '.txt'
            if not ext:
                ext = '.bin'
            
            elif 'html' in content_type:
                ext = '.html'
                
            filename = 'msg-part-%08d%s' %(counter, ext)
        counter += 1
        
        attachments.append(filename)
       
    # ======= Save File Funktionalität *** WIRD NICHT AKTIVIERT WEGEN VIRENSCHUTZ! ===================
    #         today = dt.date.today().strftime("%d%m%Y")
    #         filename = today+'__'+m_id.decode()+'__'+filename
    #         save_path = os.path.join("/1_EmailData/", "attachment_downloads", today,) #date_

    #         if save2disk == False:
    #             # return print("\nAttachments wurden NICHT abgespeichert"), attachments
    #             return attachments

    #         else:  
    #             if not os.path.exists(save_path):
    #                 os.makedirs(save_path)

    #             with open(os.path.join(save_path, filename), 'wb') as fp:
    #                 fp.write(part.get_payload(decode=True))

    #             return print(f"{len(attachments)} Attachments in {save_path} abgespeichert"), attachments
    
    return attachments



# ********************************************************************************
# uniqueMessageID
def uniqueMessageID(date,time,subject):
    '''
    Erzeugt eine unique Message ID aus Datum_Uhrzeit_ZeichenlängeBetreff
    '''
    return f"{date}_{time}_{len(subject)}"



# ********************************************************************************
# Download Funktion, die die o.g. Funktionen aufruft, die Emails vom Server liest und einen Dataframe persistiert
def readEmailBatch(con, nbr, mail_ids, anhang=False):
    
    '''
    Liest eine komplette Batch von Emails vom eingeloggten Mailserver aus und gibt diese als Dataframe zurück
    
    Input:
        con := Connection-Objekt (con),
        nbr := Numerisch oder "all" := Menge der Emails für den Download,
        mail_ids = Mail-ID Liste (z.B. mail_ids)
        
    Output:
        MAILCONTAINER := Pandas Dataframe mit den verarbeiteten Emails,
        ERROR_MID := Liste mit den fehlerhaften Mail-IDS, die im Batch-Lauf nicht gelesen werden konnten
    '''
    
    START_TIME = dt.datetime.now()
    
    if nbr == "all":
        nbr = len(mail_ids)
    
    # Dataframe Gerüst und leere Error-Log Liste erstellen
    cols="MID ENCODING TYPE DATE TIME FROM TO CC SUBJECT BODY NBR_FILES FILES MESSAGE_ID TMSTMP CTE UMID".split()
    MAILCONTAINER = pd.DataFrame(columns=cols)
    ERROR_MID = [] 

    # Auslesen der einzelnen Email anhand der mail_ids
    for m_id in mail_ids[:nbr]: 
        result, data = con.fetch(m_id,'(RFC822)')
        msg = email.message_from_bytes(data[0][1])          # Erstellt Email-Message-Objekt

        try:
            # HEADER auslesen
            header = get_header(msg)                        # Erstellt header-Dictionary aus Email-Message-Objekt
            
            encoding = decode_header(header["sub"])[0][1]   # Übergibt Tupelwert mit Encoding falls vorhanden
            subRAW = decode_header(header["sub"])[0][0]     # Übergibt Tupelwert mit encodiertertem SUBJECT
            if encoding:
                subject = subRAW.decode(encoding)           # Dekodiert SUBJECT, wenn es enkodiert vorliegt, sonst nicht 
            else:
                subject = subRAW
            
            umid = header["umid"]                           # Systemisch zugewiesener eindeutiger Identifier, aus header-Dictionary
            cte = header["cte"]                             # Zuweisung Content-Transfer-Encoding Information, aus header-Dictionary

            # message_ID = f"{header["date"]}_{header["time"]}_{len(header["sub"])}"   # WTF???
            message_ID = uniqueMessageID(header["date"], header["time"], header["sub"])
                    
            # BODY auslesen
            body = get_body(msg)                            # Liest Body (nicht dekodiert!) aus Email-Message-Objekt
            idx = m_id.decode()
            
            # Anhang INFOs auslesen. DER ANHANG WIRD NICHT ABGESPEICHERT
            if anhang:
                files = get_attachments(msg, save2disk=False)
                nbr_files = len(files)
            else:
                files = []
                nbr_files = 999

            tmstp = dt.datetime.now()                        # Verarbeitungszeitpunkt (Download) der Message

            # Pandas Dataframe befüllen
            MAILCONTAINER.loc[idx, :] = [m_id, encoding, header["ctype"], header["date"], header['time'],
                                         header["from"], header['to'], header['cc'], subject,
                                         body, nbr_files, files, message_ID, tmstp, cte, umid]
        
        # Error-Log befüllen, wenn msg nicht ausgelesen werden konnte
        except:
            ERROR_MID.append(m_id)

    # Preprocessing auf dem Dataframe
    MAILCONTAINER.ENCODING = MAILCONTAINER.ENCODING.astype("string")                                         # Datentyp ändern
    MAILCONTAINER.ENCODING.fillna("unknown", inplace=True)                                                   # NA Values in der Spalte "ENCODING" mit "unknown" ersetzen
    MAILCONTAINER["SPECFILES"] = MAILCONTAINER.FILES.apply(lambda fileListe: getSpecificFiles(fileListe) )   # Erstellt Spalte mit "Spezifischen Files"
    MAILCONTAINER["NBR_SPECFILES"] = MAILCONTAINER.SPECFILES.apply(lambda fileListe: len(fileListe))         # 
    
    DAUER = dt.datetime.now()-START_TIME
    print(f"Processing of {nbr} Emails took {DAUER}\n{len(ERROR_MID)} Message(s) with Error")
    return MAILCONTAINER, ERROR_MID



# ********************************************************************************
# Detect Encoding Tool
def detectEncodingTXT(txt):
    '''
    Hilfsfunktion: erstellt Vorschlag für Encoding Typ für den zugeführten Text (txt) in Byte-Kodierung
    Wird auf Dataframes mit .apply(lambda .: ...) eingebunden
    
    Input: txt := Textobjekt
    Output: Gibt drei Variablenwerte zurück:
        encoding := auf Basis von Chardet-KI ermitteltes Encoding der Email
        confidence := Confidence Value der Einschätzung (0 bis 1)
        language := Ermittelte Sprache (i.d.R leer)
    '''
    enc = chardet.detect(txt)
    return enc["encoding"], enc["confidence"], enc["language"]



# ********************************************************************************
# Anwedung Encoding Tool auf Dataframe
def detectEncodingDF(df):
    '''
    Erstellt Vorschlag für Encoding Typ für alle Datensätze    
    Input: df := Pandas Dataframe MAILCONTAINER mit Emailbody Spalte "BODY"
    Output: Gibt df mit drei weiteren Spalten zurück:
        df.ENCPROPOSAL := auf Basis von KI ermitteltes Encoding der Email
        df.ENCCONF := Confidence Value der Einschätzung (0 bis 1)
        df.LANGUAGE := Ermittelte Sprache (i.d.R leer)
    '''
    START_TIME = dt.datetime.now()
    df["ENCTMP"] = df.BODY.apply(lambda body: detectEncodingTXT(body))    # Detection Funktion auf Emailbody aufrufen
    df["ENCPROPOSAL"] = df.ENCTMP.apply(lambda enc: enc[0])               # Ergebnisse der Detection Function aufteilen (3x)
    df["ENCCONF"] = df.ENCTMP.apply(lambda enc: enc[1])                   # dito
    df["LANGUAGE"] = df.ENCTMP.apply(lambda enc: enc[2])                  # dito
    df.ENCPROPOSAL = df.ENCPROPOSAL.astype("string")                      # Datentyp "String" für Proposal-Spalte
    df.drop("ENCTMP", axis=1, inplace=True)                               # Redundate temporäre Spalte entfernen
    DAUER = dt.datetime.now() - START_TIME
    print(f"Processing of {df.shape[0]} Emails took {DAUER}")
    return df


# ********************************************************************************
# Email-Dekodierer (Body), Transformation von Byte-Kodierung in enkodiertes Format
def decodeEmailBody(df, errors="ignore"):
    '''
    Prozessiert einen Dataframe mit Spalte "BODY" und dekodiert
    Input: df := Pandas Dataframe MAILCONTAINER mit Emailbody Spalte "BODY"
    Output: Gibt df mit drei weiteren Spalten zurück:
        df.ENCPROPOSAL := auf Basis von KI ermitteltes Encoding der Email
        df.ENCCONF := Confidence Value der Einschätzung (0 bis 1)
        df.LANGUAGE := Ermittelte Sprache (i.d.R leer)
    '''
    
    df["BODY_DC"] = df.apply(lambda x: x["BODY"].decode(encoding=x["ENCODING"], errors=errors), axis=1)
    return df.BODY_DC


# ********************************************************************************
# Speicherfunktion für Mailcontainer (ausgelesene Emails in Dataframe)
def saveMAILCONTAINER_2disk(df, filename="Mailcontainer_"):
    '''
    Speichert 2 datumsspezifische Versionen des df auf Disk (lokales Working Directory)    
    Input:
        df := Pandas Dataframe z.B. "MAILCONTAINER",
        filename := default ist "Mailcontainer_"
        
    Output: 2 Outputfiles im Working Directory
        .pkl := Dateiformat "pickle" (binär)
        .csv := Dateiformat "csv" (csv format mit Separator \t)
    '''
    NOW = dt.datetime.now().strftime ("%d-%m-%Y")
    FN = filename+NOW
    fncsv, fnpkl = FN+".csv", FN+".pkl"
    df.to_csv(fncsv, sep="\t")
    df.to_pickle(fnpkl)
    return print(f"Successfully saved {fncsv} and {fnpkl} to disk")




# ********************************************************************************
# Dekodiere Email-Corpuse (DF)
def decodeEmailCorpus(df):
    '''
    Übernimmt enkodierten Email-Corpus mit vorab ermittelter Encoding-Information
    ... oder mit vorab ermittelter Encoding-Schätzung (Tool chardet)
    Dekodiert alle Email-Body-Texte (rows) mit der vorhandenen Encoding-Info.
    Dekodierte Email-Body-Texte werden in Spalte "BODY_DC" geschrieben
    
    Input:
        df := Email-Corpus mit Spalten "ENCODING", "BODY", "ENCPROPOSAL"
        
    Output:
        df := Email-Corpus mit dekodierten Email-Body-Texten i.d. Spalte "BODY_DC"
    '''    
    
    notDecodedYet = "=== NOT DECODED YET ==="
    df["BODY_DC"] = notDecodedYet
    count = df.shape[0]
    print(f"{count} Datensätze werden bearbeitet:\n")

    # A Dekodiere alle Email Bodys mit gesicherter Encodinginfo (ENCODING != "unknown")
    print("Decoding where Encoding is KNOWN:")
    filter_encoding = (df.ENCODING != "unknown")
    df[filter_encoding].shape
    df.loc[filter_encoding,"BODY_DC"] = df[filter_encoding].apply(lambda x: x["BODY"].decode(encoding=x["ENCODING"], errors="ignore"), axis=1)
    print(f"  Dekodiert {df[filter_encoding].shape[0]} Datensätze mit Status OK")
    print()

    # B Dekodiere alle Email Bodys mit geschätzter (!) Encodinginfo (ENCODING == "unknown")
    # =>  NA Werte aus der Encodingschätzung mit "unknown" füllen
    df.ENCPROPOSAL = df.ENCPROPOSAL.fillna("unknown")

    # =>  Alle geschätzten Encodings anwenden (encproposal_Liste)
    encproposal_Liste = df.ENCPROPOSAL.value_counts(dropna=False).index.to_list()
    filter_encoding = (df.ENCODING == "unknown")
    print("Decoding where Encoding is NOT KNOWN:")
    df[filter_encoding].shape

    for encpro in encproposal_Liste:
        filter_ =  filter_encoding & (df.ENCPROPOSAL == encpro)
        if encpro == "unknown":
            try:
                print(f"\n  Dekodiere {df[filter_].shape[0]} Datensätze ohne Encodingdetection nit UTF-8")
                df.loc[filter_, 'BODY_DC'] = df.BODY[filter_].str.decode(encoding="utf-8", errors="ignore")
                print(f"  Dekodiert {df[filter_].shape[0]} Datensätze mit Status OK")
            except:
                print(f"\n  WARNUNG {df[filter_].shape[0]} Datensätze mit Encoding: {encpro} konnten nicht dekodiert werden")

        else:
            try:
                print(f"\n  Dekodiere {df[filter_].shape[0]} Datensätze mit Encoding: {encpro}:")
                df.loc[filter_, 'BODY_DC'] = df.BODY[filter_].str.decode(encoding=encpro, errors="ignore")
                print(f"  {df[filter_].shape[0]} Datensätze mit Status OK")
            except:
                print(f"  WARNUNG {df[filter_].shape[0]} Datensätze mit Encoding: {encpro} konnten nicht dekodiert werden")

    # Eliminiere die Datensätze, die nicht dekodiert werden konnten
    mask = (df.BODY_DC != notDecodedYet)
    df = df[mask].copy()

    print(f"{count - df.shape[0]} Datensätze nicht dekodiert => werden elimiert")
    print(f"{df.shape[0]} Datensätze erfolgreich dekodiert")
    df.shape
    return df


# ********************************************************************************
# Check gegen Blacklist-Emailadressen

# Blacklist aufrufen
def loadBlacklist(path='/home/q506010/2_LookupData/'):
    '''
    Erstellt Set BLACKLIST aus gepickeltem Dataframe "BlacklistEmail.pkl"
    -------
    Input:   Optional: Path mit Dataframe
    Output:  Set BLACKLIST
    '''    
    BLpath = path
    BL = pd.read_pickle(BLpath+"BlacklistEmail.pkl")
    return {i.upper() for i in BL.KOMMUNIKATION}

# Gegen Blacklist prüfen
def checkBlacklist(txt, blacklist):
    '''
    Überprüft einen übergebenen Text, ob darin Token enthalten sind, die auf der Blacklist stehen.
    Bnötigt vor Funktionsaufruf das Preloading des Sets BLACKLIST (mit Funktion loadBlacklist())
    Funktion wird auf Dataframe.Series aufgerufen mit .apply(checkBlacklist)
    -------
    Input:   Text / String
    Output:  TRUE, wenn String Token der Blacklist enthält, sonst FALSE
    '''    
    for tok in [*nlp(txt)]:
        if tok.text.upper() in blacklist: return True
    return False
