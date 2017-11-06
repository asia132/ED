import sqlite3
import re

COMMA = re.compile("[^\s]\,[^\s]")
cur = None
sources = {
    "COUNTRIES": "./sources/countries.txt",
    "US_STATES": "./sources/us_states.txt",
    "MATCHES": "./sources/match.csv",
    "SUBCATEGORIES": "./sources/subcategories.csv",
    "CLASSES": "./sources/classes.txt",
    "ACONAME": "./sources/aconame.txt",
    "APAT": "./sources/apat63_99.txt",
    "VENTORS": "./sources/ainventor.txt",
    "CITE75_99":  "./sources/cite75_99.txt",
    "CLASS_MATCH": "./sources/class_match.txt"
}


def create_tables():
    global cur
    cur.execute("""
        CREATE TABLE COUNTRIES(
        CODE CHAR(2) PRIMARY KEY,
        COUNTRY CHAR(255));
        """)
    cur.execute("""
        CREATE TABLE US_STATES(
            CODE CHAR(2) PRIMARY KEY,
            STATE CHAR(255));
        """)
    cur.execute("""
        CREATE TABLE MATCHES(
            ASSIGNEE numeric(12),
            ASSNAME charachters(80),
            OWN numeric(5),
            PNAME   charachters(60),
            SNAME   charachters(60),
            CUSIP charachters(6),
            CNAME charachters(36));
        """)
    cur.execute("""
        CREATE TABLE SUBCATEGORIES(
            CAT numeric(2),
            SUBCAT numeric(2) PRIMARY KEY,
            SUBCATNAME charachters(60),
            CATNAMESHORT charachters(60),
            CATNAMELONG charachters(60));
        """)
    cur.execute("""
        CREATE TABLE CLASSES(
            CLASS numeric(3) PRIMARY KEY,
            TITLE charachters(255));
        """)
    cur.execute("""
        CREATE TABLE ACONAME(
            ASSIGNEE numeric(12),
            COMPNAME charachter(80),
            FOREIGN KEY (ASSIGNEE) REFERENCES MATCHES(ASSIGNEE));
        """)
    cur.execute("""
        CREATE TABLE APAT(
            PATENT      numeric(7) PRIMARY KEY,
            GYEAR       numeric(12),
            GDATE       numeric(12),
            APPYEAR     numeric(12),
            COUNTRY     numeric(3),
            POSTATE     numeric(3),
            ASSIGNEE    numeric(12),
            ASSCODE     numeric(12),
            CLAIMS      numeric(12),
            NCLASS      numeric(12),
            CAT         numeric(12),
            SUBCAT      numeric(12),
            CMADE       numeric(12),
            CRECEIVE    numeric(12),
            RATIOCIT    numeric(6),
            GENERAL     numeric(6),
            ORIGINAL    numeric(6),
            FWDAPLAG    numeric(7),
            BCKGTLAG    numeric(8),
            SELFCTUB    numeric(6),
            SELFCTLB    numeric(6),
            SECDUPBD    numeric(6),
            SECDLWBD    numeric(6),
            FOREIGN KEY (ASSIGNEE) REFERENCES MATCHES(ASSIGNEE),
            FOREIGN KEY (COUNTRY) REFERENCES COUNTRIES(CODE),
            FOREIGN KEY (SUBCAT) REFERENCES SUBCATEGORIES(SUBCAT),
            FOREIGN KEY (CAT) REFERENCES SUBCATEGORIES(CAT)
            FOREIGN KEY (POSTATE) REFERENCES US_STATES(CODE));
        """)
    cur.execute("""
        CREATE TABLE VENTORS(
            PATENT numeric(7),
            LASTNAM charachters(20),
            FIRSTNAM charachters(15),
            MIDNAM charachters(9),
            MODIFNAM charachters(3),
            STREET charachters(30),
            CITY charachters(20),
            POSTATE charachters(2),
            COUNTRY charachters(2),
            ZIP charachters(5),
            INVSEQ charachters(12),
            FOREIGN KEY (PATENT) REFERENCES APAT(PATENT),
            FOREIGN KEY (COUNTRY) REFERENCES COUNTRIES(CODE),
            FOREIGN KEY (POSTATE) REFERENCES US_STATES(CODE));
        """)
    cur.execute("""
        CREATE TABLE CITE75_99(
            CITING numeric(7),
            CITED numeric(7),
            FOREIGN KEY (CITING) REFERENCES APAT(PATENT),
            FOREIGN KEY (CITED) REFERENCES APAT(PATENT));
        """)
    cur.execute("""
        CREATE TABLE CLASS_MATCH(
            CLASS numeric(3),
            SUBCAT numeric(2),
            CAT numeric(2),
            FOREIGN KEY (SUBCAT) REFERENCES SUBCATEGORIES(SUBCAT),
            FOREIGN KEY (CLASS) REFERENCES CLASSES(CLASS),
            FOREIGN KEY (CAT) REFERENCES SUBCATEGORIES(CAT));
        """)


def drop_tables():
    global cur
    cur.execute("""DROP TABLE COUNTRIES""")
    cur.execute("""DROP TABLE US_STATES""")
    cur.execute("""DROP TABLE MATCHES""")
    cur.execute("""DROP TABLE SUBCATEGORIES""")
    cur.execute("""DROP TABLE CLASSES""")
    cur.execute("""DROP TABLE ACONAME""")
    cur.execute("""DROP TABLE APAT""")
    cur.execute("""DROP TABLE AINVENTOR""")
    cur.execute("""DROP TABLE CITE75_99""")
    cur.execute("""DROP TABLE CLASS_MATCH""")


def fill_table(table, start=1, end=-1):
    global cur
    with open(sources[table]) as f:
        lines = f.read().split('\n')[start:end]
        for line in lines:
            cur.execute(
                "INSERT INTO " + table + " VALUES(" + line + ")")


def fill_countries(table="COUNTRIES", marker='   '):
    global cur
    start = 2
    end = -1
    with open(sources[table]) as f:
        lines = f.read().split('\n')[start:end]
        for line in lines:
            code = line.split(marker)[0]
            value = line.split(marker)[1]
            cur.execute(
                "INSERT INTO " + table + " VALUES(\"" + code + "\", \"" + value + "\")")


def fill_class_matches(table="CLASS_MATCH", marker='\t'):
    global cur
    start = 1
    end = -1
    with open(sources[table]) as f:
        lines = f.read().split('\n')[start:end]
        for line in lines:
            Class = line.split(marker)[0]
            SubCat = line.split(marker)[1]
            Cat = line.split(marker)[2]
            cur.execute(
                "INSERT INTO " + table + " VALUES(\"" + Class + "\", \"" + SubCat + "\", \"" + Cat + "\")")


def fill_matches(table, start=1, end=-1):
    global cur
    pat = re.compile('\,\,')
    with open(sources[table]) as f:
        lines = f.read().split('\n')[start:end]
        for line in lines:
            while True:
                s = re.sub(pat, ',"",', line)
                if s == line:
                    break
                else:
                    line = s
            if line[-1] == ',':
                line += '""'
            cur.execute(
                "INSERT INTO " + table + " VALUES(" + line + ")")


def fill_subcategories(table="SUBCATEGORIES", start=1, end=-1):
    global cur
    pat = re.compile('\"\w+(\,[\w\s]+)+\"')
    with open(sources[table]) as f:
        lines = f.read().split('\n')[start:end]
        for line in lines:
            if pat.search(line) is None:
                list_temp = line.split(',')
            else:
                SubCatName = pat.search(line).group(0)
                line_temp = line.replace(SubCatName, '')
                list_temp = line_temp.split(',')
                list_temp[2] = SubCatName[1:-1]
            line = ''
            for elem in list_temp:
                line += '"' + elem + '", '
            line = line[:-2]
            cur.execute(
                  "INSERT INTO " + table + " VALUES(" + line + ")")


if __name__ == "__main__":
    # utworzenie polaczenia z baza przechowywana na dysku
    con = sqlite3.connect('DATA_EXPLORATION.db')
    # dostep do kolumn przez indeksy i przez nazwy
    con.row_factory = sqlite3.Row
    # utworzenie obiektu kursora
    cur = con.cursor()

    # drop_tables()
    print "Table creation..."
    create_tables()
    print "Countries loading..."
    fill_countries()
    print "States loading..."
    fill_countries(table="US_STATES")
    print "Matches loading..."
    fill_matches("MATCHES")
    print "Subcategories loading..."
    fill_subcategories()
    print "Classes loading..."
    fill_countries(table="CLASSES", marker='\t')
    print "Aconame loading..."
    fill_table("ACONAME")
    print "Apat loading..."
    fill_matches("APAT")
    print "Ainventor loading..."
    fill_table("VENTORS")
    print "Cites loading..."
    fill_table("CITE75_99")
    print "Class matches loading..."
    fill_class_matches()

    # zatwierdzamy zmiany w bazie
    con.commit()