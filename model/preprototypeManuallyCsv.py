#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 17 07:01:08 2020

@author: André Drews
@institution: TH Lübeck
@project: DiWi WiSe20/21

# Dieser Prototyp dient dazu, um fachliche Abfragen des Brazilien E-Commerce 
# Datasets auf Basis der .csv Dateien zu entwickeln.

# Grundlage sind die reports:
# report 1: Market Analysis
# report 2: Business Development
# report 3: Supply Chain
# report 4: Customer Behaviour
# report 5: Customer Satisfaction

# Die .csv Dateien werden über den DataAccessor eingelesen
# Der DataWrangler führt Operationen auf den Daten auf
# Die Reportings defininieren die fachlichen Fragestellungen

# Der Plotter plottet die Ergebnisse
# Der DocumentCreater erzeugt einen einfachen Text report

# Das Team Mitglied, das sich mit dem Fachlichen beschäftigt, kann die 
# Implementationen der Methoden für die reports abschließen (dort wo pass
# eingetragen ist)
# Sie können gerne auch erweiterete Funktionalitäten pro reports entwickeln:
# z.B. direkt auf orderreviewtexte eingehen etc.

# Die weiteren Teammitglieder müssten den postgres Anteil einbauen.
# Darüber hinaus muss die Konfiguration in .json verlagert werden 
# (reporting.json, main.json). So dass die reportings nicht mehr über
# hardcodiertes Python implementiert werden, sondern durch die .json
# Dateien konfiguriert werden. Dadurch ist der code generischer.

"""

import numpy as np
import pandas as pd
import sys
import os


PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))
#import cartopy.crs as ccrs
#import cartopy
#print(sys.path)
from matplotlib import pyplot as plt
#from src import Dummy
#from src import DataAccessor
from src import JsonParser 
#Dummy()

from src import DbOperator
from src import StatementFactory


# Datenzugriff. 
# Der db-Zugriff müsste noch implementiert werden. Gegenwärtig läuft der 
# Zugriff lediglich über .csv
class DataAccessor:
    
    def __init__(self):
        jsonParserPrePro = JsonParser("../config/program.json")
        self.main_config = jsonParserPrePro.parse() 
        jsonParserDb = JsonParser("../config/db.json")
        self.data_model = jsonParserDb.parse()
        #self.main_config = JsonParser("../config/program.json").parse()
        #self.data_model = JsonParser("../config/db.json").parse()
        self.dbOperator = DbOperator()
        self.statementFactory = StatementFactory()
        
    def getTable(self, table):
        if(self.main_config["datasource"]=="csv"):
            print("getting Data fom csv")

            return pd.read_csv(self.data_model["tables"][table]["source"])
        elif(self.main_config["datasource"]=="postgres"):
            print("getting Data fom postges")

            selectStatements = self.statementFactory.createSeletStatementByTableName(table)
            df = self.dbOperator.select(selectStatements)
            #return df #falls df schon ein dataframe ist
            return pd.DataFrame(np.array(df))
        else:
            print("data source must be either 'csv' or 'postgres'")

# Operationen/Logik auf Daten
# In dieser Prototypimplementation sind alleMethoden aus Verständlchkeitsgründen
# hart eincodiert. Dies wird noch auf eine generische Implementation umgestellt.
# Um postgres Zugang zu erlauben, muss lediglich bei jeder Methode über das
# db_config.json die "datasource" abgefragt werden und dann eine entsprechende
# if(datasource=="csv"): ... 
# elif(datasource)=="postgres"):...
# mit einer entsprechenden 
class DataWrangler:
    
    def __init__(self, dataAccessor):
        self.dataAccessor = dataAccessor
    
    def renameTable(self, table, oldfieldname, newfieldname):
        return table.rename({oldfieldname, newfieldname})
    
    ############# methods for report 1 - Market Analysis ###############
    
    # 1.0 geolocation of customer map plot
    # --> insight wo liegt die Nachfrage geographisch
    def getMergeCustomerGeolocationReducedByZipCodePrefix(self):
        customer_column_renamed = self.dataAccessor.getTable("customer").\
            rename(columns={"customer_zip_code_prefix":"zip_code_prefix"})
        geolocation_column_renamed_reduced = self.dataAccessor.getTable("geolocation").\
            rename(columns={"geolocation_zip_code_prefix":"zip_code_prefix"}).\
                drop_duplicates("zip_code_prefix")
        customer_geolocation_merged = customer_column_renamed.\
            merge(geolocation_column_renamed_reduced, on="zip_code_prefix", how="left")
        return customer_geolocation_merged
    
    # 1.1 Plot price distribution
    # --> insight: Einordnung Güter/Preise                       
    def getOrderPriceDistribution(self):
        return self.dataAccessor.getTable("order_items")["price"]
    
    # 1.2 orders per day/month/season/product  
    #  --> insight: Nachfrageverteilung innerhalb des Jahres                            
    def getGroupOrdernumbersByTimePeriod(self):
        orders_items = self.dataAccessor.getTable("orders_items")
        orders_items["order_purchase_timestamp"]=pd.to_datetime(orders_items["order_purchase_timestamp"])
        orders_items["order_delivered_customer_date"]=pd.to_datetime(orders_items["order_delivered_customer_date"])
        orders_items["order_estimated_delivery_date"]=pd.to_datetime(orders_items["order_estimated_delivery_date"])
        
    # 1.3 compute number of sellers -> Textfile
    # --> Angebotsgröße                           
    def getNumberSellers(self):
        sellers = self.dataAccessor.getTable("sellers")
        return len(sellers["seller_id"].unique())   
    # 1.4 compute number of customers -> Textfile
    # --> Nachfragegröße                    
    def getNumberOfCustomers(self):
        customers = self.dataAccessor.getTable("customer")
        return len(customers["customer_id"].unique())
    # 1.5 Compute number of orders -> Textfile
    # --> Nachfragegröße                          
    def getNumberOfOrders(self):
        orders = self.dataAccessor.getTable("order")
        return len(orders["order_id"].unique())
    
    # 1.6 Compute number of Customers by sellers
    # --> Verkäufer nach "Anteil im Markt"             
    def getNumberOfCustomersBySeller(self):
        orders = self.dataAccessor.getTable("sellers")
        return len(orders["seller_id"].unique())
    
    # 1.7 Compute number of orders
    # --> Clustern der Verkäufer nach Produktkategorie zur Verkäuferanalyse                        
    def getSellersVersusProductCatogory(self):
        pass
    
    # 1.8 Compute sellers versus product price
    # --> insights: welche sellers verkaufen besonders hochpreisig? 
    #               Wie ist der Vergleich bei gleichen Produkten 
    #               und unterschiedlichen Verkäufern?                       
    def getSellersVersusProductPrices(self):
        pass
    
    ############# methods for report 2 - Business Development ###############

    # 2.1 - compute gesamtumsatz
    # --> insight: Wie groß ist der Gesamtumsatz? Firmeneinordnung etc.
    def getTotalPrice(self):
        return self.dataAccessor.getTable("order_items")["price"].sum()
    
    # 2.2 - plot Umsatz pro Zeiteinheit
    # --> insight: Budgets, Planung, Buchhaltung, Strategieentwicklung etc.
    def getMergeOrderPriceOrderOrderedDelivered(self):
        order = self.dataAccessor.getTable("order")
        order_items = self.dataAccessor.getTable("order_items")
        order_order_items_merged = order.merge(order_items, on="order_id", how="left")
        return order_order_items_merged
   
    ############# methods for report 3 - Supply Chain ###############
    
    # 3.1 order status distribution: 
    # --> insight: wie ist die Qualität des Gesamt Order Status einzuschätzen?
    def getOrderStatusDistribution(self):
        data = self.dataAccessor.getTable("order")
        return data.groupby(by="order_status").count()["order_id"]
    
    # 3.2 sellerid versus bad status
    # --> insight: Welche Verkäufer sind besonder schlecht beim Zustellen?
    def getSellerWithBadOrderStatus(self):
        pass
    
    # 3.3 amount of delivery delay much worse than expected
    # -->  insight: wie sieht die Gesamtlage der Vorhersage aus
    def getAmountOfWrongDeliveryPredictions(self):
        pass
    
    # 3.4 paket size versus delivery date 
    # --> insight: inwiefern hat die Größe des
    # Pakets einen Einfluss auf die Zulieferzeit?
    def getInfluenceOfPaketSizeOnDeliveryTime(self):
        pass   
        
    # 3.4 delivery data delay versus geo data 
    # --> insight: in welchen Zulieferorten ist die Zustellung besonders 
    # schlecht? -> Optimierungsbedarf
    def getDeliveryDateDelayVersusGeoLocation(self):
        pass
    
    ############ methods for report 4 - Customer Behaviour  ###############
    
    # 4.1 PaymenttypeüberPreis scatter plot   
    # --> insight: Welche Bezahlart sollte z.B. als erstes auf 
    # Web site erscheinen
    def getPaymentTypeForDifferentPrices(self):
        pass
    
    # 4.2. review answer speed
    # --> insight: wie schnell kann mit review Antworten gerechnet werden?
    def getReviewAnswerSpeed(self):
        pass
    
    # 4.3. welcher Kunde bestellt welche Ware wie oft? --> 
    # --> insight: Kundenclustering. Daraus ließen sich Recommendation 
    # Strategien ableiten
    def getCustomerProductBehaviour(self):
        pass
    
    ########## report 5 - Customer Setisfaction +++++++++++++++++++++++++

    # 5.1. score distribution (histogram) - insight: wie ist die Gesamtzufriedenheit einzuschätzen?
    def getGeneralCustomerSatisfaction(self):
        pass
    
    # 5.2. Score versus seller (line) 
    # - insight: bei welchen Verkäufern sind Kunden besonders unzufrieden?
    def getReviewScoreVersusSeller(self):
        pass
    
    # 5.3. Score versus product (histogram) 
    #- insight: bei welchen Produkten` sind Kunden besonders unzufrieden?
    def getReviewScoreVersusProduct(self):
        pass
    
    # 5.4. Score versus delivery time
    # insight: wie stark ist die delivery time  auf die customer satisfaction
    def getReviewVersusVersusDeliveryTime(self):
        pass


class Plotter:
    
    def __init__(self):
        pass
    
#    def plotMap(self, extend, title1, title2, datalon, datalat):
#        fig, ax = plt.subplots(subplot_kw={"projection":ccrs.PlateCarree()}, figsize=(20,15))
#        ax = plt.axes(projection=ccrs.PlateCarree())
#        ax.set_extent(extend, ccrs.PlateCarree())
#        ax.coastlines(resolution='110m')
#        ax.add_feature(cartopy.feature.BORDERS)
#        ax.add_feature(cartopy.feature.RIVERS)
#        plt.scatter(datalon, datalat, s=1)
#        ax.set_title(title2, fontsize=30)
#        fig.suptitle(title1, fontsize=40)
        
    def plotHistInRange(self, data, bins, xleft, xright, xlabel, title):
        plt.figure(figsize=(20,15))
        plt.hist(data, bins=bins)
        plt.xlim(xleft,xright)
        plt.title(title, fontsize=40)
        plt.gca().set_xlabel(xlabel, fontsize=30)
        plt.gca().set_ylabel("Amount (A.U.)", fontsize=30)
        plt.gca().tick_params(labelsize=30)
        
    def plotBar(self, data, height, xlabel, title):
        plt.figure(figsize=(20,15))
        plt.bar(data, height=height)
        plt.title(title, fontsize=40)
        plt.gca().set_xlabel(xlabel, fontsize=30)
        plt.gca().set_ylabel("Amount (A.U.)", fontsize=30)
        plt.gca().tick_params(labelsize=30)
        
    def plotPie(self, labels, sizes, explore, title):
        fig = plt.figure(figsize=(20,15))
        plt.gca().pie(sizes, explode=explore, labels=labels, autopct='%1.1f%%',
        shadow=True, startangle=90, textprops={"fontsize":30})
       # plt.legend(pie[0],labels, bbox_to_anchor=(1,0.5), loc="center right", fontsize=10, 
        #   bbox_transform=plt.gcf().transFigure)
        plt.gca().tick_params(labelsize=30)
        fig.suptitle(title, fontsize=40)

        
    def plotHist(self, data, bins, title):
        plt.figure()
        plt.hist(data, bins=bins)
        plt.title(title)
        
        
class OutputManager:
    def __init__(self, reportId):
        self.outputfolderPlot = "output/data/plots/" + reportId
        self.outputfolderText = "output/data/text/" + reportId
        
    def saveFig(self, figurename):
        figure=plt.gcf()
        figure.savefig(self.outputfolderPlot + "/" + figurename + ".png")
        
    def saveAsDocumentation(self):
        pass
        

class Report_1_Market_Analysis:
    
    def __init__(self, dataWrangler, plotter):
        self.dataWrangler = dataWrangler
        self.plotter = plotter
        self.outputManager = OutputManager("report_1")
        self.name = "Report 1 Market Analysis"
        
    def execute(self):
        
        # 1.1 geolocation of customer map plot
        # --> insight wo liegt die Nachfrage geographisch
        geoData = self.dataWrangler.getMergeCustomerGeolocationReducedByZipCodePrefix()
        self.plotter.plotMap([-100, -20, 20, -40], \
                            "Geolocation of Customers","Map section: longitude: (-100, -20), Latitude: (20, -40)", \
                                 geoData["geolocation_lng"], geoData["geolocation_lat"])
        self.outputManager.saveFig("geoLocationCustomer")
        
        # 1.2 Plot price distribution
        # --> insight: Einordnung Güter/Preise                       
        priceDistribution = self.dataWrangler.getOrderPriceDistribution()
        self.plotter.plotHistInRange(priceDistribution, 1000, 0, 1000, "Price (Brazilian Real Equals ~ 0.16 EUR)", "Price Distribution of Orders")
        self.outputManager.saveFig("totalPriceDistributionOfOrders")
        
        # 1.3 orders per day/month/season/product  
        #  --> insight: Nachfrageverteilung innerhalb des Jahres                            
        #self.dataWranglergetGroupOrdernumbersByTimePeriod()  
        
        # 1.4 plot number of sellers, customers and orders
        # --> Angebots- und Nachfragegröße    -> Textfile                        
        number_sellers = self.dataWrangler.getNumberSellers()                   
        number_customers = self.dataWrangler.getNumberOfCustomers()
        number_orders = self.dataWrangler.getNumberOfOrders()
        numbers = [number_sellers, number_customers, number_orders]
        self.plotter.plotBar(["sellers", "customers", "orders"], numbers, "",  "Total number sellers, customers, orders")
        self.outputManager.saveFig("totalNumberSellersCustomersOrders")
        # 1.5 Plots customers by sellers
        # --> Verkäufer nach Kundenanzahl, Maß für die Marktposition des Verkäufers´            
        #self.dataWrangler.getNumberOfCustomersBySeller()
    
        # 1.6 Plot sellers versus Product Category
        # --> Clustern der Verkäufer nach Produktkategorie zur Verkäuferanalyse                        
        #self.getSellersVersusProductCatogory()
        
        # 1.8 Compute sellers versus product price
        # --> insights: welche sellers verkaufen besonders hochpreisig? 
        #               Wie ist der Vergleich bei gleichen Produkten 
        #               und unterschiedlichen Verkäufern?                       
        #self.getSellersVersusProductPrices()
    
    def printName(self):
        print("+++++ " + self.name + " ++++")
    
    
class Report_2_Business_Development:
    
    def __init__(self, dataWrangler, plotter):
        self.dataWrangler = dataWrangler
        self.plotter = plotter
        self.OutputManager = OutputManager("report_2")
        self.name = "Report 2 Buiness Development"
        
    def execute(self):
        pass
        # 2.1 - plot gesamtumsatz und Gesamtumsatz abzüglich nicht zustande
        #       gekommene Kaufverträge (Lieferengpässe etc.) barplot
        # --> insight: Wie groß ist der Gesamtumsatz? Firmeneinordnung etc.
        #self.dataWrangler.getTotalPrice()
        
        # 2.2 - plot Umsatz pro Zeiteinheit
        # --> insight: Budgets, Planung, Buchhaltung, Strategieentwicklung etc.
        #self.dataWrangler.getMergeOrderPriceOrderOrderedDelivered()
        
        
    def printName(self):
        print("+++++" + self.name + "++++")
   
    
class Report_3_Supply_Chain:
    
    def __init__(self, dataWrangler, plotter):
        self.dataWrangler = dataWrangler
        self.plotter = plotter
        self.outputManager = OutputManager("report_3")
        self.name = "Report 3 Supply Chain"
        
    def execute(self):        
        # 3.1 order status distribution: 
        # --> insight: wie ist die Qualität des Gesamt Order Status einzuschätzen?
        orderstatusDistribution = self.dataWrangler.getOrderStatusDistribution()
        self.plotter.plotPie(orderstatusDistribution.index, orderstatusDistribution, [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7], "Order Status")
        self.outputManager.saveFig("orderStatus")
        
        #self.plotter.plotPie(orderstatusDistribution, "Order Status Distribution")
        
        # 3.2 sellerid versus bad status
        # --> insight: Welche Verkäufer sind besonder schlecht beim Zustellen?
        self.dataWrangler.getSellerWithBadOrderStatus()
        
        # 3.3 amount of delivery delay much worse than expected
        # -->  insight: wie sieht die Gesamtlage der Vorhersage aus
        self.dataWrangler.getAmountOfWrongDeliveryPredictions()
        
        # 3.4 paket size versus delivery date 
        # --> insight: inwiefern hat die Größe des
        # Pakets einen Einfluss auf die Zulieferzeit?
        self.dataWrangler.getInfluenceOfPaketSizeOnDeliveryTime()
            
        # 3.4 delivery data delay versus geo data 
        # --> insight: in welchen Zulieferorten ist die Zustellung besonders 
        # schlecht? -> Optimierungsbedarf
        self.dataWrangler.getDeliveryDateDelayVersusGeoLocation()
        
    def printName(self):
        print("+++++ " + self.name + " ++++")
    
    
class Report_4_Customer_Behaviour:
    
    def __init__(self, dataWrangler, plotter):
        self.dataWrangler = dataWrangler
        self.plotter = plotter
        self.OutputManager = OutputManager("report_4")
        self.name = "Report 4 Customer Behaviour"
        
    def execute(self):
        # 4.1 PaymenttypeüberPreis scatter plot   
        # --> insight: Welche Bezahlart sollte z.B. als erstes auf 
        # Web site erscheinen
        self.dataWrangler.getPaymentTypeForDifferentPrices()
        
        # 4.2. review answer speed
        # --> insight: wie schnell kann mit review Antworten gerechnet werden?
        self.dataWrangler.getReviewAnswerSpeed()
        
        # 4.3. welcher Kunde bestellt welche Ware wie oft? --> 
        # --> insight: Kundenclustering. Daraus ließen sich Recommendation 
        # Strategien ableiten
        self.dataWrangler.getCustomerProductBehaviour()
        
    def printName(self):
        print("+++++ " + self.name + " ++++")
   
    
class Report_5_Customer_Satisfaction:
    
    def __init__(self, dataWrangler, plotter):
        self.dataWrangler = dataWrangler
        self.plotter = plotter
        self.OutputManager = OutputManager("report_5")
        self.name = "Report 5 Customer Satisfaction"
        
    def execute(self):    
        # 5.1. score distribution (histogram) - insight: wie ist die Gesamtzufriedenheit einzuschätzen?
        self.dataWrangler.getGeneralCustomerSatisfaction()
        
        # 5.2. Score versus seller (line) 
        # - insight: bei welchen Verkäufern sind Kunden besonders unzufrieden?
        self.dataWrangler.getReviewScoreVersusSeller()
        
        # 5.3. Score versus product (histogram) 
        #- insight: bei welchen Produkten` sind Kunden besonders unzufrieden?
        self.dataWrangler.getReviewScoreVersusProduct()
        
        # 5.4. Score versus delivery time
        # insight: wie stark ist die delivery time  auf die customer satisfaction
        self.dataWrangler.getReviewVersusVersusDeliveryTime()
    
    def printName(self):
        print("+++++ " + self.name + " ++++")
        
        
        
class ProcessorManually:
    def __init__(self, reportlist):
        self.reportlist = reportlist

    def execute(self):
        for report in self.reportlist:
            report.printName()
            report.execute()
        
            

if __name__=="__main__":  
    report1 = Report_1_Market_Analysis(DataWrangler(DataAccessor()), Plotter())
    #report2 = Report_2_Business_Development(DataWrangler(DataAccessor()), Plotter())
    #report3 = Report_3_Supply_Chain(DataWrangler(DataAccessor()), Plotter())
    #report4 = Report_4_Customer_Behaviour(DataWrangler(DataAccessor()), Plotter())
    #report5 = Report_5_Customer_Satisfaction(DataWrangler(DataAccessor()), Plotter())

    #processor = ProcessorManually([report1, report3])
    #processor.execute()
    