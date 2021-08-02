# -*- coding: utf-8 -*-
"""
Created on Mon Jul 26 11:53:50 2021

@author: arnaud.ceragioli
"""

from datetime import datetime
import pandas as pd
from tqdm import tqdm
from pymongo import MongoClient 
from class_Test import *

"""
from bokeh.io import output_file, show
from bokeh.models import ColumnDataSource, FactorRange
from bokeh.plotting import figure
from bokeh.transform import factor_cmap
"""

from matplotlib import pyplot as plt
import numpy as np

#Connect database
myclient = MongoClient("mongodb+srv://ArnaudCERA:a1z2e3r4@clustertest.p2fh9.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
myclient = MongoClient("localhost")
# database && collection 
db = myclient["online_retail"]
collection = db["retail_data"]


df = pd.read_excel("Online Retail.xlsx", sheet_name="Online Retail")  
#Faire tests unitaires en amont
df.dropna(subset=["CustomerID", "Quantity", "UnitPrice"], inplace=True)
df["StockCode"] = df["StockCode"].astype(str)
df["CustomerID"] = df["CustomerID"].astype(str)

list_data = []

for index, rows in tqdm(df.iterrows()):
    my_row = {"InvoiceNo":rows.InvoiceNo, "StockCode":rows.StockCode, "Description":rows.Description, "Quantity":rows.Quantity, "InvoiceDate":rows.InvoiceDate, "UnitPrice":rows.UnitPrice, "CustomerID":rows.CustomerID, "Country":rows.Country}
    list_data.append(my_row)

collection.insert_many(list_data)

retail_data = pd.DataFrame(list(collection.find()))


#Step 1: Group all transactions by InvoiceNo
result = list(collection.aggregate(
    [
         { "$group": 
              { "_id": "$InvoiceNo", "numberOfElements": { "$sum": 1 } }
         }   
         ,{ "$sort" : { "_id" : 1 } }
    ]))

gk = retail_data.groupby("InvoiceNo")
    
if testItFirst(result, gk) == True:
    print("First test passed")

#Step 2: Which product sold the most
result2 = list(collection.aggregate(
    [
         { "$group": 
              { "_id": "$StockCode", 
               "sumBoughtItems": 
                   { "$sum": "$Quantity"}, 
               "description": 
                   {"$first" : "$Description"} } },
        
         { "$sort" : { "sumBoughtItems" : -1 } }           
    ]))[0]

#print(result2)
gk2 = retail_data.groupby(["StockCode"])
max_score = 0
name = ""
for gkd in gk2:
    values = gkd[1].sum(skipna = True).get("Quantity")

    if values > max_score :
        max_score = values
        name = gkd[0]
    
if testItSecond(result2, max_score, name) == True:
    print("Second test passed")

#Step 3: Which customer spent the most money
result3 = list(collection.aggregate(
    [
         { "$group": 
              { "_id": "$CustomerID", "moneySpent":
                                          { "$sum":
                                               { "$multiply":
                                                    [ "$UnitPrice", "$Quantity" ] } }},},
         { "$match" : { "moneySpent" : { "$gte" : 0 } } },
         { "$sort" : { "moneySpent" : -1 } }           
    ]))[0]

#print(result3)

gk3 = retail_data.groupby(["CustomerID"])
max_score3 = 0
name3 = ""

for gkd in gk3:
    values = (gkd[1]["Quantity"]*gkd[1]["UnitPrice"]).sum()
    if values > max_score3:
        max_score3 = values
        name3 = gkd[0]
    
if testItThird(result3, max_score3, name3) == True:
    print("Third test passed")

#Step 4: Distribution of each product for each available country
list_of_countries = collection.distinct("Country")
list_of_products = list()
list_of_amounts = list()

#output_file("bar_nested_colormapped.html")

result4 = list(collection.aggregate(
    [
         { "$group": { "_id": ["$StockCode", "$Country"], "amountItems": { "$sum": "$Quantity" } } },         
         { "$match" : { "amountItems" : { "$gte" : 0 } } },
         { "$sort" : { "amountItems" : -1 } }
    ]))


gk4 = retail_data.groupby(["StockCode", "Country"])

dico_values = dict()
list_values = list()

for gkd in gk4:
    values = gkd[1]["Quantity"].sum()
    if values >= 0:
        dico_values["_id"] = [gkd[1]["StockCode"].iloc[0], gkd[1]["Country"].iloc[0]]
        dico_values["amountItems"] = values
        list_values.append(dico_values.copy())

list_values = sorted(list_values, key=lambda k: k['amountItems'], reverse=True)
    
if testItFourth(result4, list_values) == True:
    print("Fourth test passed")


#Use Bokeh Library to display data
"""

dico_elem_per_country = dict()
for res in result4:
    if res["amountItems"] >= 0 :       
        list_of_products.append(res["_id"][0]) 
        list_of_amounts.append(res["amountItems"])
        
        for country in list_of_countries:
            if country not in dico_elem_per_country:
                dico_elem_per_country[country] = list()
            if res["_id"][1] == country:
                dico_elem_per_country[country].append(res["amountItems"])
            else:
                dico_elem_per_country[country].append(0) #np.nan
   
                
data = {'products' : list_of_products}
new_tuple = tuple()

for country in list_of_countries:
    data[country] = dico_elem_per_country[country]
    new_tuple += tuple(dico_elem_per_country[country])               
        
palette = ["#c9d9d3", "#718dbf", "#e84d60","#c9d9d3", "#718dbf", "#e84d60","#c9d9d3", "#718dbf", "#e84d60",
           "#c9d9d3", "#718dbf", "#e84d60","#c9d9d3", "#718dbf", "#e84d60","#c9d9d3", "#718dbf", "#e84d60",
           "#c9d9d3", "#718dbf", "#e84d60","#c9d9d3", "#718dbf", "#e84d60","#c9d9d3", "#718dbf", "#e84d60"
           "#c9d9d3", "#718dbf", "#e84d60","#c9d9d3", "#718dbf", "#e84d60","#c9d9d3", "#718dbf", "#e84d60", "#e84d60"]                

print(type(new_tuple))
x = [ (product, country) for product in list_of_products for country in list_of_countries ]
#counts = sum(zip(new_tuple), ()) # like an hstack
print(type(counts))

source = ColumnDataSource(data=dict(x=x, counts=counts))

p = figure(x_range=FactorRange(*x), plot_height=750, title="Products sold per country",
           toolbar_location=None, tools="")

p.vbar(x='x', top='counts', width=0.9, source=source, line_color="white",
       fill_color=factor_cmap('x', palette=palette, factors=list_of_countries, start=1, end=2))

p.y_range.start = 0
p.x_range.range_padding = 0.1
p.xaxis.major_label_orientation = 1
p.xgrid.grid_line_color = None

show(p)


"""

#Step 5: Distribution of each product for each available country
result5 = list(collection.aggregate(
    [
         { "$group": { "_id":"_id", "AverageValue": { "$avg": "$UnitPrice" } } }      
    ]))[0]

gk5 = retail_data["UnitPrice"].mean()
    
if testItFifth(result5, gk5) == True:
    print("Average unit price according to whole database is:", str(result5["AverageValue"]))
    print("Fifth test passed")


#Step 6: Chart showing distribution of prices
result6 = list(collection.aggregate(
    [
         { "$group": { "_id": ["$StockCode", "$Country"], "priceDistribution": { "$avg": "$UnitPrice" } } },     
         { "$sort" : { "priceDistribution" : -1 } }
    ]))

gk6 = retail_data.groupby(["StockCode", "Country"])

dico_values = dict()
list_values = list()

for gkd in gk6:
    values = gkd[1]["UnitPrice"].mean()
    dico_values["_id"] = [gkd[1]["StockCode"].iloc[0], gkd[1]["Country"].iloc[0]]
    dico_values["priceDistribution"] = values
    list_values.append(dico_values.copy())

list_values = sorted(list_values, key=lambda k: k['priceDistribution'], reverse=True)
    
if testItSixth(result6, list_values) == True:
    print("Sixth test passed")
#Can show graph bc too much data


#Step 7: Ratio between price and quantity for each invoice
result7 = list(collection.aggregate(
    [
         { "$group": 
              { "_id": "$InvoiceNo" ,
               "sumUnitPrice":
                    { "$sum": "$UnitPrice" },
               "sumQuantity":
                    { "$sum": "$Quantity" } }, }, 
         { "$project": {"sumQuantity":1, "sumUnitPrice":1, "RatioUnit": { "$divide": [ "$sumUnitPrice", "$sumQuantity" ]}}},
         { "$match" : { "RatioUnit" : { "$gte" : 0 } } },
         { "$sort" : { "RatioUnit" : -1 } }
         
    ]))

gk7 = retail_data.groupby("InvoiceNo")

dico_values = dict()
list_values = list()

for gkd in gk7:
    values_sumPrice = gkd[1]["UnitPrice"].sum()
    values_sumQuant = gkd[1]["Quantity"].sum()
    ratio = values_sumPrice/values_sumQuant
    if ratio >= 0 :
        dico_values["_id"] = gkd[0]
        dico_values["RatioUnit"] = ratio
        dico_values["sumQuantity"] = values_sumQuant
        dico_values["sumUnitPrice"] = values_sumPrice
        list_values.append(dico_values.copy())
        
list_values = sorted(list_values, key=lambda k: k['RatioUnit'], reverse=True)
    
if testItSeventh(result7, list_values) == True:
    print("Seventh test passed")


#Step 8: Display how many transactions each country has
result8 = list(collection.aggregate(
    [
         { "$group": { "_id": "$Country" , "listOfInvoices": { "$addToSet": "$InvoiceNo" } }, },
         { "$project": {"transactionsPerCountry": { "$size": "$listOfInvoices"}}},
         { "$match": { "_id" : { "$ne" : "Unspecified" }}},
         { "$sort" : { "transactionsPerCountry" : -1, "_id": -1 } }
    ]))

gk8 = retail_data.groupby("Country")

dico_values = dict()
list_values = list()

for gkd in gk8:
    if gkd[0] != "Unspecified" :
        dico_values["_id"] = gkd[0]
        dico_values["transactionsPerCountry"] = len(gkd[1]["InvoiceNo"].unique().tolist())
        list_values.append(dico_values.copy()) 
        
list_values = sorted(list_values, key=lambda k: (k['transactionsPerCountry'], k['_id']), reverse=True)   

if testItEigth(result8, list_values) == True:
    print("Eigth test passed")

    list_of_countries = list()
    list_of_soldPerCountry = list()
    
    for res in result8:
        list_of_countries.append(res["_id"])
        list_of_soldPerCountry.append(res["transactionsPerCountry"])
        
      
    # Creating plot
    fig = plt.figure(figsize =(30, 21))
    plt.pie(list_of_soldPerCountry, labels = list_of_countries, autopct='%1.2f%%'
            ,textprops = {"fontsize":30}, shadow=True)
      
    # show plot
    plt.legend()    
    plt.show()


#Step 9: Create relevant groups based on their name

#Import Dataset
X = pd.DataFrame(retail_data.iloc[:, [4, 6, 8]].values)

#Manage missing Data
""" Data have been already managed at the beginning by deleting empty data 
(they couldn't have been filled with median or average of their data)"""
 
#Manage categorical data
test = pd.get_dummies(X[2],drop_first=True)

X = pd.concat([X, test], axis=1)
X = X.drop(2, axis=1)

X = X.values

# Utiliser la méthode elbow pour trouver le nombre optimal de clusters
from sklearn.cluster import KMeans
wcss = []
for i in range(1, 11):
    kmeans = KMeans(n_clusters = i, init = 'k-means++', random_state = 0)
    kmeans.fit(X)
    wcss.append(kmeans.inertia_)
plt.plot(range(1, 11), wcss)
plt.title('La méthode Elbow')
plt.xlabel('Nombre de clusters')
plt.ylabel('WCSS')
plt.show()
#We take the value where the elbow is

# Construction du modèle
from sklearn.cluster import KMeans
kmeans = KMeans(n_clusters = 3, init = 'k-means++', random_state = 0)
y_kmeans = kmeans.fit_predict(X)

# Visualiser les résultats 
"""
plt.scatter(X[y_kmeans == 1, 0], X[y_kmeans == 1, 1], c = 'red', label = 'Cluster 1')
plt.scatter(X[y_kmeans == 2, 0], X[y_kmeans == 2, 1], c = 'blue', label = 'Cluster 2')
plt.scatter(X[y_kmeans == 3, 0], X[y_kmeans == 3, 1], c = 'green', label = 'Cluster 3')
plt.title('Cluster de noms de produits')
plt.xlabel('Country')
plt.ylabel('Ratio')
plt.legend()
"""
#==========================================


