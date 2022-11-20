import csv
import itertools
import sqlite3
import xml.etree.ElementTree as ET


# Question 1
def create_tables(connexion):
    cur = connexion.cursor()

    # 7
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Regions (code_region STRING, nom_region STRING, nombre_arrondissements INTEGER,"
        "nombre_cantons INTEGER, nombre_communes INTEGER, population_municipale INTEGER, population_totale INTEGER)")
    connexion.commit()

    # 9
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Departements (code_region STRING, nom_region STRING, code_departement STRING,"
        "nom_departement STRING, nombre_arrondissements INTEGER, nombre_cantons INTEGER, nombre_communes INTEGER,"
        "population_municipale INTEGER, population_totale INTEGER)")
    connexion.commit()

    # 9
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Communes (code_region STRING, nom_region STRING, code_departement STRING,"
        "code_arrondissement INTEGER, code_canton INTEGER, code_commune INTEGER, nom_commune STRING, population_municipale INTEGER,"
        "population_a_part STRING, population_totale INTEGER)")
    connexion.commit()

    cur.close()


# Question 1
def fill_region_from_csv(connexion):
    with open('csv_files/regions.csv', 'r') as f:
        lines = f.readlines()[8:]
    cur = connexion.cursor()
    for line in lines:
        line = line.strip()
        line = line.split(';')
        clean_numbers = [int(line[i].replace(' ', '')) if line[i] else 0 for i in range(2, 7)]
        cur.execute("INSERT INTO Regions VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (line[0],
                     line[1],
                     clean_numbers[0],
                     clean_numbers[1],
                     clean_numbers[2],
                     clean_numbers[3],
                     clean_numbers[4]))
    connexion.commit()


# Question 1
def fill_departement_from_csv(connexion):
    with open('csv_files/departements.csv', 'r') as f:
        lines = f.readlines()[8:]
    cur = connexion.cursor()
    for line in lines[:-1]:
        line = line.strip()
        line = line.split(';')
        cleaned_numbers = [int(line[i].replace(' ', '')) if line[i] else 0 for i in range(4, 9)]
        cur.execute("INSERT INTO Departements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (line[0],
                     line[1],
                     line[2],
                     line[3],
                     cleaned_numbers[0],
                     cleaned_numbers[1],
                     cleaned_numbers[2],
                     cleaned_numbers[3],
                     cleaned_numbers[4]))
    connexion.commit()


# Question 1
def fill_commune_from_csv(connexion):
    with open('csv_files/communes.csv', 'r') as f:
        lines = f.readlines()[8:]
    cur = connexion.cursor()
    for line in lines[:-1]:
        line = line.strip()
        line = line.split(';')
        cleaned_numbers = [int(line[i].replace(' ', '')) if line[i] else 0 for i in
                           itertools.chain(range(3, 6), range(7, 10))]
        cur.execute("INSERT INTO Communes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (line[0],
                     line[1],
                     line[2],
                     cleaned_numbers[0],
                     cleaned_numbers[1],
                     cleaned_numbers[2],
                     line[6],
                     cleaned_numbers[3],
                     cleaned_numbers[4],
                     cleaned_numbers[5]))
    connexion.commit()


# Question 2 : Régions
def compute_regions_population(connexion):
    cur = connexion.cursor()
    cur.execute("SELECT nom_region, SUM(population_totale) FROM Communes GROUP BY code_region")
    return cur.fetchall()


# Question 2 : Régions
def get_regions_population(connexion):
    cur = connexion.cursor()
    cur.execute("SELECT nom_region, population_totale FROM Regions ORDER BY code_region")
    return cur.fetchall()


# Question 2 : Régions
def compare_regions_population(connexion):
    regions_population = get_regions_population(connexion)
    regions_population_computed = compute_regions_population(connexion)
    for computed_region, region in zip(regions_population_computed, regions_population):
        print(
            f"{computed_region[0]:30}: {computed_region[1]:#,} vs {region[1]:#,} {'OK' if computed_region[1] == region[1] else 'ERROR'}")


# Question 2 : Départements
def compute_departement_population(connexion):
    cur = connexion.cursor()
    cur.execute("SELECT SUM(population_totale) FROM Communes GROUP BY code_departement")
    return cur.fetchall()


# Question 2 : Départements
def get_departement_population(connexion):
    cur = connexion.cursor()
    cur.execute("SELECT nom_departement, population_totale FROM Departements ORDER BY code_departement")
    return cur.fetchall()


# Question 2 : Départements
def compare_departement_population(connexion):
    departement_population = get_departement_population(connexion)
    departement_population_computed = compute_departement_population(connexion)
    for computed_departement, departement in zip(departement_population_computed, departement_population):
        print(
            f"{departement[0]:30}: {computed_departement[0]:#,} vs {departement[1]:#,} {'OK' if computed_departement[0] == departement[1] else 'ERROR'}")


# Question 3
def get_communes_with_same_name_and_different_departements(connexion):
    all_communes = dict()
    expected_final_result = dict()

    cur = connexion.cursor()
    cur.execute("SELECT nom_commune, code_departement FROM Communes ORDER BY nom_commune")

    for commune in cur.fetchall():
        all_communes[commune[0]] = all_communes.get(commune[0], []) + [commune[1]]

    for commune, departements in all_communes.items():
        if len(departements) > 1:
            expected_final_result[commune] = departements

    return expected_final_result


# Question 4
def save_database_to_xml(connexion):
    cur = connexion.cursor()
    cur.execute("SELECT * FROM Regions")
    regions = cur.fetchall()
    cur.execute("SELECT * FROM Departements")
    departements = cur.fetchall()
    cur.execute("SELECT * FROM Communes")
    communes = cur.fetchall()

    root = ET.Element('root')
    regions_element = ET.SubElement(root, 'regions')
    departements_element = ET.SubElement(root, 'departements')
    communes_element = ET.SubElement(root, 'communes')

    for region in regions:
        region_element = ET.SubElement(regions_element, 'region')
        ET.SubElement(region_element, 'code_region').text = str(region[0])
        ET.SubElement(region_element, 'nom_region').text = region[1]
        ET.SubElement(region_element, 'nombre_arrondissements').text = str(region[2])
        ET.SubElement(region_element, 'nombre_cantons').text = str(region[3])
        ET.SubElement(region_element, 'nombre_communes').text = str(region[4])
        ET.SubElement(region_element, 'population_municipale').text = str(region[5])
        ET.SubElement(region_element, 'population_totale').text = str(region[6])

    for departement in departements:
        departement_element = ET.SubElement(departements_element, 'departement')
        ET.SubElement(departement_element, 'code_region').text = str(departement[0])
        ET.SubElement(departement_element, 'nom_region').text = departement[1]
        ET.SubElement(departement_element, 'code_departement').text = str(departement[2])
        ET.SubElement(departement_element, 'nom_departement').text = str(departement[3])
        ET.SubElement(departement_element, 'nombre_arrondissements').text = str(departement[4])
        ET.SubElement(departement_element, 'nombre_cantons').text = str(departement[5])
        ET.SubElement(departement_element, 'nombre_communes').text = str(departement[6])
        ET.SubElement(departement_element, 'population_municipale').text = str(departement[7])
        ET.SubElement(departement_element, 'population_totale').text = str(departement[8])

    for commune in communes:
        commune_element = ET.SubElement(communes_element, 'commune')
        ET.SubElement(commune_element, 'code_region').text = str(commune[0])
        ET.SubElement(commune_element, 'nom_region').text = commune[1]
        ET.SubElement(commune_element, 'code_departement').text = str(commune[2])
        ET.SubElement(commune_element, 'code_arrondissement').text = str(commune[3])
        ET.SubElement(commune_element, 'code_canton').text = str(commune[4])
        ET.SubElement(commune_element, 'code_commune').text = str(commune[5])
        ET.SubElement(commune_element, 'nom_commune').text = str(commune[6])
        ET.SubElement(commune_element, 'population_municipale').text = str(commune[7])
        ET.SubElement(commune_element, 'population_a_part').text = str(commune[8])
        ET.SubElement(commune_element, 'population_totale').text = str(commune[9])

    tree = ET.ElementTree(root)
    tree.write('database.xml')


# Question 4
def fill_database_from_xml(connexion):
    cur = connexion.cursor()
    tree = ET.parse('database.xml')
    root = tree.getroot()
    for region in root[0]:
        cur.execute("INSERT INTO Regions VALUES (?, ?, ?, ?, ?, ?, ?)", (
            region[0].text, region[1].text, region[2].text, region[3].text, region[4].text, region[5].text,
            region[6].text))
    for departement in root[1]:
        cur.execute("INSERT INTO Departements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (
            departement[0].text, departement[1].text, departement[2].text, departement[3].text, departement[4].text,
            departement[5].text, departement[6].text, departement[7].text, departement[8].text))
    for commune in root[2]:
        cur.execute("INSERT INTO Communes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
            commune[0].text, commune[1].text, commune[2].text, commune[3].text, commune[4].text, commune[5].text,
            commune[6].text, commune[7].text, commune[8].text, commune[9].text))
    connexion.commit()


# Question 5
def add_new_regions_column_to_departements(connexion):
    csv_line = []
    with open('csv_files/nouvelles_regions_2.csv', 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        next(reader)
        for row in reader:
            csv_line.append(row)

    cur = connexion.cursor()
    cur.execute("ALTER TABLE Departements ADD COLUMN nom_nouvelle_region TEXT")
    cur.execute("ALTER TABLE Departements ADD COLUMN code_nouvelle_region INTEGER")
    for line in csv_line:
        cur.execute("UPDATE Departements SET nom_nouvelle_region = ? WHERE nom_region = ?", (line[1], line[0]))
        cur.execute("UPDATE Departements SET code_nouvelle_region = ? WHERE nom_region = ?", (int(line[2]), line[0]))
    connexion.commit()


# Question 5
def compute_new_regions_population(connexion):
    cur = connexion.cursor()
    cur.execute(
        "SELECT nom_nouvelle_region, code_nouvelle_region, SUM(population_totale) FROM Departements GROUP BY code_nouvelle_region")
    new_regions = cur.fetchall()

    return new_regions


if __name__ == '__main__':
    conn = sqlite3.connect('database.sqlite')

    # Question 1
    create_tables(connexion=conn)
    # fill_region_from_csv(connexion=conn)
    # fill_departement_from_csv(connexion=conn)
    # fill_commune_from_csv(connexion=conn)

    # Question 2
    # compare_regions_population(connexion=conn)
    # compare_departement_population(connexion=conn)

    # Question 3
    # for communes, departement in get_communes_with_same_name_and_different_departements(connexion=conn).items():
    #     print(f"Commune : {communes:20} Départements : {departement}")

    # Question 4
    # save_database_to_xml(connexion=conn)
    # fill_database_from_xml(connexion=conn)

    # Question 5
    # add_new_regions_column_to_departements(connexion=conn)
    # print(f"{Fore.LIGHTYELLOW_EX}{'Nouvelle région':40} {'Nouveau Code'}\t {'Population'}")
    # for regions in compute_new_regions_population(connexion=conn):
    #     print(
    #         f"{Fore.LIGHTCYAN_EX}{regions[0]:40} {Fore.MAGENTA}{regions[1]}\t\t\t\t {Fore.GREEN}{regions[2]:,}{Style.RESET_ALL}")

    conn.close()
