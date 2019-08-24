# Dependencies
from splinter import Browser
from splinter.exceptions import ElementDoesNotExist
from bs4 import BeautifulSoup
import pymongo
import os
import pandas as pd
    import numpy as np
import re
import datetime
import time
import json

os.chdir("C:/Users/gutie/Dropbox/Documentos Insumos/PREWORK_CGF/GitLab/TECMC201905DATA2/Week 13 - ETL Project/Project")

conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)
db = client.News_db
collection0 = db.Lista_Notas
collection1 = db.Notas_txt

municipios=pd.read_csv("CatalogoMunicipios.csv", delimiter=",", encoding='utf-8')
municipios=municipios[municipios.Cve_Ent==12].reset_index()

head=[]
link=[]
resumen=[]
fecha=[]
municipio=[]

executable_path = {'executable_path': 'chromedriver.exe'}
browser = Browser('chrome', **executable_path, headless=False)

t=0
for mun in municipios.Nom_Mun:

    url0='https://suracapulco.mx/impreso/?s='
    url1=url0+mun
    browser.visit(url1)
    time.sleep(5)
    html = browser.html
    soup = BeautifulSoup(html, 'html.parser')
    page=soup.find_all('div', class_='nav-links')
    nPages=pd.DataFrame(pd.to_numeric(page[0].text.split(), errors='coerce')).max()
    if(int(nPages)>50):
        nPages=50
    url0='https://suracapulco.mx/impreso/page/'
    
    for pg in range(1,(int(nPages)+1)):
            url_l=url0+str(pg)+"/?s="+mun
            browser.visit(url_l)
            time.sleep(1.5)
            html = browser.html
            soup = BeautifulSoup(html, 'html.parser')
            Notas= soup.find_all('div', class_='search_main')
            for nota in Notas:
                t=t+1
                if nota.find('a')['href'] is None: link.append("") 
                else: link.append(nota.find('a')['href'])
                if nota.find('time')['datetime'] is None: fecha.append("")  
                else: fecha.append(nota.find('time')['datetime'][:10])
                if nota.find('h2') is None: head.append("") 
                else: head.append(nota.find('h2').text.strip())
                if nota.find('p') is None: resumen.append("") 
                else: resumen.append(nota.find('p').text.strip())
                municipio.append(mun)
                if (t<5): print(f"n.{t}:------------\nFecha: {nota.find('time')['datetime'][:10]}\nHeader: {nota.find('h2').text.strip()}")
                

Notas_df=pd.DataFrame({'link':link, 'fecha':fecha, 'municipio':municipio, 'header':head, 'resumen':resumen})
Notas_df.fecha=pd.to_datetime(Notas_df.fecha, format='%Y-%m-%d')
Notas_df.to_csv("DB_News.csv")

df=Notas_df[["link", "fecha", "header", "resumen"]]
df.drop_duplicates(subset ="link", keep = 'first', inplace = True)

#Load data to mongo DB
records = json.loads(Notas_df.to_json(orient='records'))
collection0.insert_many(records, ordered=False)

#Query data
post = db.Notas.find()
t=0
for pt in post:
   t=t+1
   if(t<15): print(pt)
#df=Notas_df.copy()
#df["oid"]=df.groupby('link').cumcount()
#df=df.pivot(index="link", columns='oid', values='municipio')


nota=[]
link_id=[]
t=0
for nt in df.link:
    t=t+1
    browser.visit(nt)
    time.sleep(1.5)
    html = browser.html
    soup = BeautifulSoup(html, 'html.parser')
    NT=soup.find('div', class_="details-content").text
    if soup.find('div', class_="details-content").text is None: nota.append("")
    else: nota.append(NT)
    if soup.find('div', class_="details-content").text is None: link_id.append(nt)
    else: link_id.append(nt)
    print(f'{t}:-----------------')
    if(t<10): print(f'link: {nt} \nNota: {NT}')

Articulos=pd.DataFrame({'link':link_id, 'Nota_tx':nota})
            
violencia=["cartel","crimen","delincuencia organizada","ama de fuego", "homicidio", "mueto", "ejecución", "ejecutado", "balacera", "enfrentamiento", "violencia", "desaparecid", "homicidio"]
politica=["candidat", "política", "cabildo", "presidente", "diputado", "gobernador", "iniciativa de ley", "regidor", "director de"]

Articulos["Violencia"]=0
Articulos["Politica"]=0


for n in range(len(Articulos.index)):
    for vl in violencia:
        if re.search(vl, Articulos.Nota_tx[n]) is not None: Articulos.Violencia[n]=1

for n in range(len(Articulos.index)):
    for pl in politica:
        if re.search(pl, Articulos.Nota_tx[n]) is not None: Articulos.Politica[n]=1



records = json.loads(Articulos.to_json(orient='records'))
collection.insert_many(records, ordered=False)
post = db.Notas.find()
t=0
for pt in post:
   t=t+1
   if(t<15): print(pt)