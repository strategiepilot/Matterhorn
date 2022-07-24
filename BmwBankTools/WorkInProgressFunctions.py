"""
Work in Progress Bibliothek
Autor: Andreas Barth

"""

# ********************************************************************************
def checkLookUps(text):
    tokenizer = nlp.Defaults.create_tokenizer(nlp)
    toks = [t for t in tokenizer(text) if t.is_punct == False]
    clean_tokens = []
  
    for tok in toks:
        toktest = tok.text.upper().strip()
        toklen = len(tok.text)
       
        if toklen < 10 and toktest in PLZZAHLEN:
            tok = "<PLZ>"
        elif toklen < 7 and toktest in VINSSHORT:
            tok = "<VIN>"
        elif toklen > 7 and toktest in VINSLONG:
            tok = "<VIN>"
        elif toktest in VORNAMEN:
            tok = "<VORNAME>"
        elif toktest in NACHNAMEN:
            tok = "<NACHNAME>"
        elif toktest in STAEDTE:
            tok = "<STADT>"
        elif toktest in STRASSEN:
            tok = "<STRASSE>"
        
        clean_tokens.append(tok)
    return " ".join([str(t) for t in clean_tokens])


# ********************************************************************************
def loadLookUpData():
    path = '/home/q506010/2_LookupData/'

    # Load Source Dataframes and drop NA-values
    vornamenDF = pd.read_pickle(path+"VornamenXXL_DF.pkl").dropna()
    nachnamenDF = pd.read_pickle(path+"NachnamenXXL_DF.pkl").dropna()
    staedteDF = pd.read_pickle(path+"StaedteXXL_DF.pkl").dropna().str.rstrip()
    strassenDF = pd.read_pickle(path+"StrassenXXL_DF.pkl").dropna()
    plzDF = pd.read_pickle(path+"Plz_DF.pkl").dropna()
    vinShortDF = pd.read_pickle(path+"VIN-Short_DF.pkl").dropna()
    vinLongDF = pd.read_pickle(path+"VIN-Long_DF.pkl").dropna()
    
    # Hilfskontrukte
    verbenDF = pd.read_pickle(path+"Verben_DE_DF.pkl").dropna()
    adjektiveDF = pd.read_pickle(path+"Adjektive_DE_DF.pkl").dropna()
    praepositionenDF = pd.read_pickle(path+"Praepositioinen_DE_DF.pkl").dropna()
    pronomenDF = pd.read_pickle(path+"Pronomen_DE_DF.pkl").dropna()
    
    # Transform Dataframes into sets for look-Up Performance
    VORNAMEN = {i.upper() for i in vornamenDF.tolist() if len(i)>3}
    NACHNAMEN = {i.upper() for i in nachnamenDF.tolist() if len(i)>3}
    STAEDTE = {i.upper() for i in staedteDF.tolist() if len(i)>3}
    STRASSEN = {i.upper() for i in strassenDF.tolist() if len(i)>3}
    PLZZAHLEN = {i.upper() for i in plzDF.tolist()}
    VINSSHORT = {i.upper() for i in vinShortDF.tolist()}
    VINSLONG = {i.upper() for i in vinLongDF.tolist()}
    BICCODES = set()  # Dummy-Set ... ggf. durch echtes Set ersetzen, wenn BIC-Daten vorhanden sind
    
    VERBEN = {i.upper() for i in verbenDF.tolist()}
    ADJEKTIVE = {i.upper() for i in adjektiveDF.tolist()}
    PRAEPOSITIONEN = {i.upper() for i in praepositionenDF.Prepositions.tolist()}
    PRONOMEN = {i.upper() for i in pronomenDF.tolist()}

    # Eliminate Overlaps in sets
    NACHNAMEN = NACHNAMEN.difference(VERBEN, ADJEKTIVE, PRAEPOSITIONEN, PRONOMEN)
    VORNAMEN = VORNAMEN.difference(NACHNAMEN, STAEDTE, VERBEN, ADJEKTIVE, PRAEPOSITIONEN, PRONOMEN)
    STAEDTE = STAEDTE.difference(NACHNAMEN, VORNAMEN, VERBEN, ADJEKTIVE, PRAEPOSITIONEN, PRONOMEN)
    PLZZAHLEN = PLZZAHLEN.difference(STAEDTE)
    
    print(
        f"Länge: \nVORNAMEN: {len(VORNAMEN)}, NACHNAMEN: {len(NACHNAMEN)},\
        \nSTRASSEN: {len(STRASSEN)}, PLZs: {len(PLZZAHLEN)}, STAEDTE: {len(STAEDTE)}, \
        \nVINS-SHORT: {len(VINSSHORT)}, VINS-LONG: {len(VINSLONG)}, BICCODES: {len(BICCODES)}"
    )
    
    return VORNAMEN, NACHNAMEN, STRASSEN, PLZZAHLEN, STAEDTE, VINSSHORT, VINSLONG #, BICCODES

# ********************************************************************************
t1 = 'Damit Ihr indess erkennt, woher dieser ganze Irrthum gekommen ist, Stoppmarke und weshalb man die Lust anklagt und den Schmerz lobet, so will ich Euch Alles eröffnen und auseinander setzen, was jener Begründer der Wahrheit und gleichsam Baumeister des glücklichen Lebens selbst darüber gesagt hat. Niemand, sagt er, verschmähe, oder hasse, oder fliehe die Lust als solche, sondern weil grosse Schmerzen ihr folgen, wenn man nicht mit Vernunft ihr nachzugehen verstehe. Ebenso werde der Schmerz als solcher von Niemand geliebt, gesucht und verlangt, sondern weil mitunter solche Zeiten eintreten, dass man mittelst Arbeiten und Schmerzen eine grosse Lust sich zu verschaften suchen müsse. Um hier gleich bei dem Einfachsten stehen zu bleiben, so würde Niemand von uns anstrengende körperliche Übungen vornehmen, wenn er nicht einen Vortheil davon erwartete. Wer dürfte aber wohl Den tadeln, der nach einer Lust verlangt, welcher keine Unannehmlichkeit folgt, oder der einem Schmerze ausweicht,Diese Nachricht wurde von meinem [Android Mobiltelefon mit WEB.DE <http://WEB.DE> Mail] gesendet Am 11.05.20, 13:20 schrieb bmw.bank@bmw.de:Damit Ihr indess erkennt, woher dieser ganze Irrthum gekommen ist, Stoppmarke und weshalb man die Lust anklagt und den Schmerz lobet, so will ich Euch Alles eröffnen und auseinander setzen, was jener Begründer der Wahrheit und gleichsam Baumeist'
t2 = 'Weit hinten, hinter den Wortbergen, fern der Länder Vokalien und Konsonantien leben die Blindtexte. Abgeschieden wohnen sie in Buchstabhausen an der Küste des Semantik, eines großen Sprachozeans. Ein kleines Bächlein namens Duden fließt durch ihren Ort und versorgt sie mit den nötigen Regelialien. Es ist ein paradiesmatisches Land, in dem einem gebratene Satzteile in den Mund fliegen. Nicht einmal von der allmächtigen Interpunktion werden die Blindtexte beherrscht – ein geradezu unorthographisches Leben. Eines Tages aber beschloß eine kleine Zeile Blindtext, ihr Name war Lorem Ipsum, hinaus zu gehen in die weite Grammatik. Der große Oxmox riet ihr davon ab, da es dort wimmele von bösen Kommata, wilden Fragezeichen und hinterhältigen Semikoli, doch das Blindtextchen ließ sich nicht beirren. Es packte seine sieben Versalien, schob sich sein Initial in den Gürtel und machte sich auf den Weg. Als es die ersten Hügel des Kursivgebirges erklommen hatte, warf es einen letzten Blick zurück auf die Skyline seiner Heimatstadt Buchstabhausen, die Headline von Alphabetdorf und die Subline seiner eigenen Straße, der Zeilengasse.'