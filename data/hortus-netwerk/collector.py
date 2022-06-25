import os
import sys
import requests
import re
import logging
import time
import csv
from bs4 import BeautifulSoup

MAX_PAGES=100
PLANT_LINKS_FILE="plantlinks.txt"

logging.basicConfig(filename='collector.log', encoding='utf-8', filemode='w', level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())

logging.info("Starting collector...")

# Create Index
os.makedirs("index", exist_ok=True)

def getLinks(page):
    links = []
    soup = BeautifulSoup(page, features="html.parser")
    for article in soup.find_all('article'):
        link = article.find('a')['href']
        logging.info(f"Found link: {link}")
        links.append(link)
    return links

overviewExists = os.path.isfile(PLANT_LINKS_FILE)

if not overviewExists:
    pageIndex = 1   
    maximumPages = MAX_PAGES + pageIndex
    successfulRequest = True

    plantLinks = []
    
    while (successfulRequest == True) and (pageIndex < maximumPages):
        url = f'https://hortus-netzwerk.de/category/pflanzendatenbank/page/{pageIndex}/'
        logging.debug(f'Get URL: {url}')
        res = requests.get(url)
        if res.status_code == 200:
            filename = f'index/page{pageIndex}.html'
            with open(filename, "w") as page:
                content = res.content.decode("utf-8")
                page.write(content)
                plantLinks.extend(getLinks(content))
                logging.info(f'File for Page {pageIndex} written: {filename}')

        else:
            successfulRequest = False
            logging.info(f'Request Failed for Page {pageIndex} with URL: {url}')

        pageIndex = pageIndex + 1
        time.sleep(0.1)

    logging.info(f'Total of {pageIndex} Pages loaded with {len(plantLinks)} Links')
        
    with open(PLANT_LINKS_FILE, "w") as plantLinksFile:
        for link in plantLinks:
            plantLinksFile.write(link+"\n")

# Parse Plant Links

os.makedirs("plants", exist_ok=True)

KNOWN_COLUMNS=[
    "Deutscher Name / Handelsname",
    "Botanischer Name",
    "Familie",
    "Herkunft",
    "Vegetationsperiode",
    "Höhe",
    "Bevorzugter Standort",
    "Feuchtigkeitsanspruch",
    "Bevorzugter Bode",
    "Blütezeitraum",
    "Blütenfarbe",
    "Geeignete Zone",
    "Sonstiges / Bemerkunge",
    "Verwendbarkeit",
    "Nutze",
    "Nutzen für",
    #Self Defined for Images
    "Images"
]

COLUMN_TRANSLATION=[
    "Name",
    "BotanicName",
    "Family",
    "Herkunft",
    "GrowingSeason",
    "Height",
    "Location",
    "HumidityRequirement",
    "Soil",
    "FloweringPeriod",
    "FlowerColor",
    "SuitableZone",
    "Notes",
    "Usability",
    "Benefit",
    "Benefit",
    "Images"
]

plantDataList = []

def processData(plantname, content):
    soup = BeautifulSoup(content, features="html.parser")
    plant_data = {}
    for row in soup.find_all("tr", { "class": re.compile("row*") }):
        columns = row.find_all("td")
        if len(columns) < 1:
            columns = row.find_all("th")

        column_name = ""
        column_data = ""
    
        if row.find("figure"):
            column_name = "Images"
            column_data = []
            for figure in row.find_all("figure"):
                for image in figure.find_all("img"):
                    logging.info("Added Image")
                    column_data.append(image["src"])
        else:
            try:
                column_name = columns[0].text.strip("n\t")
                column_name = column_name.translate({ord("\n"): " ", ord("\t"): " "})
                column_data = columns[1].text.strip("\n\t")
                column_data = column_data.translate({ord("\n"): " ", ord("\t"): " "})
            except:
                logging.error(f'Could not parse {plantname}')
                continue

        if column_name == "" or column_data == "":
            logging.error(f'Empty name or data for column of {plantname}')
            continue

        try:
            index = KNOWN_COLUMNS.index(column_name)
            column_name = COLUMN_TRANSLATION[index]
        except:
            if "©" not in column_name:
                logging.error(f'Unknown Column: {column_name}')
                continue
            continue # we don't wont the ones with © in our data

        plant_data[column_name] = column_data

    plantDataList.append(plant_data)

with open(PLANT_LINKS_FILE, "r") as plantLinksFile:
    plantLinks = plantLinksFile.readlines()
    logging.info(f'Found {len(plantLinks)} Plants')
    for link in plantLinks:
        link = link.strip("\n")
        plantname = link.split("/")[3:4][0]
        logging.debug(f'Get Plant {plantname} with URL: {link}')
        filename = f'plants/{plantname}.html'
        if os.path.isfile(filename):
            with open(filename, "r") as cache_file:
                processData(plantname, cache_file.read())
        else: 
            res = requests.get(link)
            if res.status_code == 200:
                logging.debug(f'Filename for Plant: {filename}')
                with open(filename, "w") as page:
                    content = res.content.decode("utf-8")
                    page.write(content)
                    processData(plantname, content)
                    logging.info(f'File for Plant {plantname} written: {filename}')

            else:
                logging.error(f'Request Failed for Plant {link} with StatusCode {res.status_code}')
                sys.exit(0)

            time.sleep(0.1) # only sleep if we use the server

# Write CSV

def read_csv_to_dict(file_path):
    with open(file_path) as f:
        dict = [{column: value for column, value in row.items()} for row in csv.DictReader(f, skipinitialspace=True, delimiter=',')]
    return dict

#data = read_csv_to_dict('sample.csv')
#print(data[0].keys())

def write_dict_to_csv(file_path, list_of_dicts):
    keys = set()
    for dict in list_of_dicts:
        keys.update(dict.keys())
    with open(file_path, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, skipinitialspace=True, delimiter=',', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

write_dict_to_csv("../hortus_netzwerk_plant_data_collected.csv", plantDataList)
