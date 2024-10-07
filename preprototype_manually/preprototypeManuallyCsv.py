#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sa 18:23 27.02.2021

@author: André Drews, Ben Alexy, Hendrik Garken, Jokab Poley und Robin Schumacher
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

import pandas as pd

import json_parser

import cartopy.crs as ccrs
import cartopy

from matplotlib import pyplot as plt

import time
import datetime
import numpy as np

from configparser import ConfigParser
import psycopg2


""" 
 class DbOperator
 DbOperator connects to database via ../config/database.ini and 
 executes postgres statements or selects data based on ../conf/db.json

 @institution: TH Luebeck
 @author: Jakob Poley
 @date: 23.01.2021 
"""

class DbOperator:


    def __init__(self):
        self.params = self.config()


    # setting config parameters for database connection
    # @param filename: path to db-access config file database.ini
    # @param section: postgres section (if in future further databases are allowed)
    def config(self, filename='config/database.ini', section='postgresql'):
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(filename)

        # get section, default is postgresql (see section='postgresql' in line 28)
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))

        return db


    # selects data
    # @param query: query string
    def select(self, query):
        
        #Connect to the PostgreSQL database server
        
        conn = None
        try:
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**self.params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            cur.execute(query)
            
            # display the postgreSQL select content 
            content = cur.fetchall()

            # close the communication with the PostgreSQL
            cur.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
                return content




""" 
 class StatementFactory 
 creates SQL-Statements to get data from the local database

 @institution: TH Luebeck
 @author: Jakob Poley
 @date: 23.01.2021 
"""
class StatementFactory:


    # gets data form postges
    # @param
    def createSelectStatement(self, datamodel):
        statement = {}
        for table in datamodel["tables"]:
            statement[table]="SELECT * FROM " + table + ";"
        return statement

    def createSeletStatementByTableName(self, tablename, fields="*"):
        return "SELECT " + fields +" FROM " +tablename



# Datenzugriff. 
# in der main.json kann ausgewählt werden ob der Datenzugriff über die csv-Dateien
# oder die lokele PostgreSQL Datenbank gescehen soll 
class DataAccessor:
    
    def __init__(self):
        self.main_config = json_parser.JsonParser("config/main.json").parse()
        self.db_config = json_parser.JsonParser("config/db.json").parse()
        self.dbOperator = DbOperator()
        self.statementFactory = StatementFactory()
        
    def getTable(self, table):
        if(self.main_config["datasource"]=="csv"):
            print("getting Data fom csv-file")
            return pd.read_csv(self.db_config["tables"][table]["source"])
        elif(self.main_config["datasource"]=="postgres"):
            print("getting Data fom postges")

            selectStatements = self.statementFactory.createSeletStatementByTableName(table)
            listFromDb = self.dbOperator.select(selectStatements)
            df = pd.DataFrame(listFromDb)
            for i, column in enumerate(self.db_config["tables"][table]["fields"]):
                df = df.rename(columns={i: column})
            
            return df
        else:
            print("data source must be either 'csv' or 'postgres'")



class DataWrangler:
    
    def __init__(self, dataAccessor):
        self.dataAccessor = dataAccessor
    
    def renameTable(self, table, oldfieldname, newfieldname):
        return table.rename({oldfieldname, newfieldname})
    
    ############# methods for report 1 - Market Analysis ###############
    
    # 1.0 geolocation of customer map plot
    # --> insight wo liegt die Nachfrage geographisch
    def getMergeCustomerGeolocationReducedByZipCodePrefix(self):
        customer_column_renamed = self.dataAccessor.getTable("customers").\
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
    # @author: Robin Schumacher                         
    def getGroupOrdernumbersByTimePeriod(self):
        orders_times = self.dataAccessor.getTable("orders")[["order_id", "order_purchase_timestamp"]]
        product_id = self.dataAccessor.getTable("order_items")[["order_id", "product_id"]]
        orders_times["order_purchase_timestamp"] = pd.to_datetime(orders_times["order_purchase_timestamp"])
        orders_times["Day_order"] = orders_times["order_purchase_timestamp"].dt.day
        orders_times["Month_order"] = orders_times["order_purchase_timestamp"].dt.month
        orders_times["Year_order"] = orders_times["order_purchase_timestamp"].dt.year
        orders_times["Dayofweek_order"] = orders_times["order_purchase_timestamp"].dt.dayofweek
        seasons = [1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 1]
        month_to_season = dict(zip(range(1,13), seasons))
        orders_times["Season_order"] = orders_times["order_purchase_timestamp"].dt.month.map(month_to_season) #Seasons: Spring = 2, Summer = 3, Autumn = 4, Winter = 1
        orders_times = orders_times.sort_values(by = "order_purchase_timestamp")
        merge = pd.merge(left = orders_times, right = product_id, how='left', on='order_id').dropna()
        return merge
        
    # 1.3 compute number of sellers -> Textfile
    # --> Angebotsgröße                           
    def getNumberSellers(self):
        sellers = self.dataAccessor.getTable("sellers")
        return len(sellers["seller_id"].unique())  

    # 1.4 compute number of customers -> Textfile
    # --> Nachfragegröße                    
    def getNumberOfCustomers(self):
        customers = self.dataAccessor.getTable("customers")
        return len(customers["customer_id"].unique())

    # 1.5 Compute number of orders -> Textfile
    # --> Nachfragegröße                          
    def getNumberOfOrders(self):
        orders = self.dataAccessor.getTable("orders")
        return len(orders["order_id"].unique())
    
    # 1.6 Compute number of Customers by sellers
    # --> Verkäufer nach "Anteil im Markt"
    # author: Ben Alexy             
    def getSalesBySeller(self):
        sellers = self.dataAccessor.getTable("sellers")
        orderitems = self.dataAccessor.getTable("order_items")
        orderitemssellersmerged = orderitems.merge(sellers, on = "seller_id", how="left")
        return orderitemssellersmerged.groupby(by = "seller_id").sum()["price"]

    
    # 1.7 Compute number of orders
    # --> Clustern der Verkäufer nach Produktkategorie zur Verkäuferanalyse
    # author: Ben Alexy                        
    def getSellersVersusProductCatogory(self):
        #zunächst die Relationen holen
        order_items = self.dataAccessor.getTable("order_items")
        order_category = self.dataAccessor.getTable("products")
        #dann die Relationen joinen über product_id
        order_items_order_category_merged = order_items.merge(order_category, on="product_id", how="left" )
        #und dann das Dataframe gruppieren nach der seller_id und dort pro seller_id die Produktkategorien zählen lassen
        return order_items_order_category_merged.groupby(by = "seller_id").count()["product_category_name"]

    # 1.8 Compute sellers versus product price
    # --> insights: welche sellers verkaufen besonders hochpreisig? 
    #               Wie ist der Vergleich bei gleichen Produkten  
    #               und unterschiedlichen Verkäufern?    
    # @author: Robin Schumacher                   
    def getSellersVersusProductPrices(self):
        product = self.dataAccessor.getTable("order_items")
        dupes_without_seller = product.drop_duplicates(subset = ["seller_id", "product_id"], keep = "first").sort_values(by = "product_id")
        only_dupes = dupes_without_seller[dupes_without_seller.duplicated(["product_id"], keep = False)].sort_values(by = "product_id")
        average_price = only_dupes.groupby('product_id', as_index= False).mean().drop(columns=['freight_value', 'order_item_id']).\
          rename(columns={"price":"average_price"})
        merge = pd.merge(left = only_dupes, right = average_price, how='left', on = ["product_id"])
        merge["price_difference"] = ((merge["price"] - merge["average_price"])/merge["average_price"])
        merge = merge.sort_values(by = "price_difference")
        merge = merge.drop(columns=['order_id', 'order_item_id', 'product_id', 'price', 'freight_value', 'average_price', 'shipping_limit_date', 'order_id'])
        merge = merge.groupby(by ='seller_id').mean()
        return merge


    ############# methods for report 2 - Business Development ###############

    # 2.1 - compute gesamtumsatz
    # --> insight: Wie groß ist der Gesamtumsatz? Firmeneinordnung etc.
    # @author: Robin Schumacher
    def getTotalPrice(self):
        price = self.dataAccessor.getTable("order_items")[["order_id", "price"]]
        totalsales = price["price"].sum()
        totalsales = round(totalsales, 2)
        order = self.dataAccessor.getTable("orders")[["order_id", "order_status"]]
        cancelled_order = order[(order.order_status != "delivered") & (order.order_status != "invoiced") & (order.order_status != "shipped") & (order.order_status != "processing")]
        failed_sum = pd.merge(left = price, right = cancelled_order, how='left', on = ["order_id"])
        failed_sum = failed_sum.dropna()
        failed_sum = failed_sum["price"].sum()
        difference = totalsales - failed_sum
        merged = [totalsales, failed_sum, difference]
        return merged
    
    # 2.2 - plot Umsatz pro Einheit
    # --> insight: Budgets, Planung, Buchhaltung, Strategieentwicklung etc.
    # @author: Robin Schumacher
    def getMergeOrderPriceOrderOrderedDelivered(self):
        orders_times = self.dataAccessor.getTable("orders")[["order_id", "order_purchase_timestamp"]]
        product_id = self.dataAccessor.getTable("order_items")[["order_id", "product_id", "price"]]
        orders_times["order_purchase_timestamp"] = pd.to_datetime(orders_times["order_purchase_timestamp"])
        orders_times['Day_order'] = orders_times["order_purchase_timestamp"].dt.day
        orders_times["Dayofweek_order"] = orders_times["order_purchase_timestamp"].dt.dayofweek
        orders_times['Month_order'] = orders_times["order_purchase_timestamp"].dt.month
        orders_times = orders_times.sort_values(by = "order_purchase_timestamp")
        orders_times['Year_order'] = orders_times["order_purchase_timestamp"].dt.year
        seasons = [1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 1]
        month_to_season = dict(zip(range(1,13), seasons))
        orders_times['Season_order'] = orders_times['order_purchase_timestamp'].dt.month.map(month_to_season) #Seasons: Spring = 2, Summer = 3, Autumn = 4, Winter = 1
        merge = pd.merge(left = orders_times, right = product_id, how='left', on='order_id').dropna()
        return merge

   
    ############# methods for report 3 - Supply Chain ###############
    
    # 3.1 order status distribution: 
    # --> insight: wie ist die Qualität des Gesamt Order Status einzuschätzen?
    # @author: Hendrik Garken
    def getOrderStatusDistribution(self):
        order = self.dataAccessor.getTable("orders")
        order_without_delivered = order[order["order_status"]!="delivered"]
        return order_without_delivered    
      
    # 3.2 sellerid versus bad status
    # --> insight: Welche Verkäufer sind besonder schlecht beim Zustellen?
    # @author: Hendrik Garken

    def getSellerWithBadOrderStatus(self):
        order_items = self.dataAccessor.getTable("order_items")
        orders = self.dataAccessor.getTable("orders")
        sellerpunctuality = orders.merge(order_items, on="order_id", how="left")
        sellerpunctuality["difftime"] =  pd.to_datetime(sellerpunctuality["order_estimated_delivery_date"]) \
            - pd.to_datetime(sellerpunctuality["order_delivered_customer_date"])
                
        sellerpunctuality_group = sellerpunctuality.groupby(by="seller_id")["difftime"].groups
        values = []
        for key in sellerpunctuality_group:
             values.append(np.mean(sellerpunctuality_group[key])/3600)
        return values
            
           
          
    # 3.3 amount of delivery delay much worse than expected
    # -->  insight: wie sieht die Gesamtlage der Vorhersage aus
    # @author: Robin Schumacher
    def getAmountOfWrongDeliveryPredictions(self):
        sellerpunctuality= self.dataAccessor.getTable("orders")[["order_status", "order_estimated_delivery_date", "order_delivered_customer_date"]]
        sellerpunctuality = sellerpunctuality.loc[sellerpunctuality["order_status"] == "delivered"]
        sellerpunctuality["difftime"] =  pd.to_datetime(sellerpunctuality["order_estimated_delivery_date"]) \
            - pd.to_datetime(sellerpunctuality["order_delivered_customer_date"])
        sellerpunctuality["difftime"] = sellerpunctuality["difftime"].astype('timedelta64[D]')
        negative = (sellerpunctuality["difftime"] < 0).sum()
        zero = (sellerpunctuality["difftime"] == 0).sum() 
        one_two = ((sellerpunctuality["difftime"] == 2).sum()) + ((sellerpunctuality["difftime"] == 1).sum()) 
        three_five = ((sellerpunctuality["difftime"] < 6).sum()) - one_two - zero - negative 
        six_ten = ((sellerpunctuality["difftime"] < 11).sum()) - three_five - one_two - zero - negative
        ten_twenty = ((sellerpunctuality["difftime"] < 21).sum()) - six_ten - three_five - one_two - negative
        twenty_fifty = ((sellerpunctuality["difftime"] < 51).sum()) - ten_twenty - six_ten - three_five - one_two- zero - negative
        fifty_end = (sellerpunctuality["difftime"] > 50).sum()
        merged = [negative, zero, one_two, three_five, six_ten, ten_twenty, twenty_fifty, fifty_end]
        return merged
    
    # 3.4 paket size versus delivery date 
    # --> insight: inwiefern hat die Größe des Pakets einen Einfluss auf die Zulieferzeit?
    # @author: Jakob Poley
    def getInfluenceOfPaketSizeOnDeliveryTime(self):
        products = self.dataAccessor.getTable("products")
        products["volume_l"] =  products["product_length_cm"] * products["product_height_cm"] * products["product_width_cm"] * (1/1000)
        products = products.drop(columns=["product_category_name" , "product_name_lenght" , "product_description_lenght" , "product_photos_qty", "product_weight_g" , "product_length_cm" , "product_height_cm" , "product_width_cm"])
        
        orderItems = self.dataAccessor.getTable("order_items")
        orderItems = orderItems.drop(columns=["seller_id" , "shipping_limit_date" , "price" , "freight_value", "order_item_id"])

        orders = self.dataAccessor.getTable("orders")
        orders["deliverytime"] =  pd.to_datetime(orders["order_delivered_customer_date"]) \
            - pd.to_datetime(orders["order_purchase_timestamp"])
        orders["deliverytime"] = orders["deliverytime"].astype('timedelta64[D]')
        orders = orders.drop(columns=["customer_id" , "order_status" , "order_approved_at" , "order_delivered_carrier_date"])
        
        p_oi_merge = pd.merge(products, orderItems, how='left', on='product_id')
        p_oi_o_merge = pd.merge(p_oi_merge, orders, how='left', on='order_id')
        return p_oi_o_merge
        
    # 3.5 delivery data delay versus geo data 
    # --> insight: in welchen Zulieferorten ist die Zustellung besonders schlecht? -> Optimierungsbedarf
    # @author: Jakob Poley
    def getDeliveryDateDelayVersusGeoLocation(self):
        customers = self.dataAccessor.getTable("customers")
        orders = self.dataAccessor.getTable("orders")

        merge_orders_on_customers = pd.merge(orders, customers, how='left', on='customer_id')
        merge_orders_on_customers["difftime"] =  pd.to_datetime(merge_orders_on_customers["order_estimated_delivery_date"]) \
            - pd.to_datetime(merge_orders_on_customers["order_delivered_customer_date"])

        merge_orders_on_customers["difftime"] = merge_orders_on_customers["difftime"].astype('timedelta64[D]')
        statenames = ["AC","AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"] 
        mean_difftime_per_state = []
        for state in statenames:
            state = merge_orders_on_customers.loc[merge_orders_on_customers["customer_state"]== state]
            state = state["difftime"].mean()
            mean_difftime_per_state.append(state)
        return mean_difftime_per_state

    ############ methods for report 4 - Customer Behaviour  ###############
    
    # 4.1 most used payments  
    # --> insight: Welche Bezahlart ist am beliebtesten
    # @author: Robin Schumacher
    def getNumberOfPaymenttypes(self):
        payment_type = self.dataAccessor.getTable("payments")["payment_type"]
        boleto = payment_type[payment_type == "boleto"].count()
        credit_card = payment_type[payment_type == "credit_card"].count()
        debit_card = payment_type[payment_type == "debit_card"].count()
        not_defined = payment_type[payment_type == "not_definied"].count()
        voucher = payment_type[payment_type == "voucher"].count()
        merged = [boleto, credit_card, debit_card, not_defined, voucher]
        return merged
    
    # 4.2. review answer speed
    # --> insight: wie schnell kann mit review Antworten gerechnet werden?
    # @author: Robin Schumacher
    def getReviewAnswerSpeed(self):
        answer_time = self.dataAccessor.getTable("reviews")[["review_creation_date", "review_answer_timestamp"]]
        answer_time["review_answer_speed"] = pd.to_datetime(answer_time["review_answer_timestamp"]) - pd.to_datetime(answer_time["review_creation_date"])
        answer_time = answer_time["review_answer_speed"].astype('timedelta64[h]')
        return answer_time
    
    # 4.3 PaymenttypeüberPreis   
    # --> insight: Welche Bezahlart wird bei welchem Einkaufswert am häufigsten genutzt
    # @author: Robin Schumacher
    def getPaymenttypeüberPreis(self):
        payment_type = self.dataAccessor.getTable("payments")[["payment_type", "payment_value"]]
        return payment_type

    
    ########## report 5 - Customer Setisfaction +++++++++++++++++++++++++

    # 5.1. score distribution 
    # - insight: wie ist die Gesamtzufriedenheit einzuschätzen?
    # @author: Robin Schumacher
    def getGeneralCustomerSatisfaction(self):
        order_reviews = self.dataAccessor.getTable("reviews")["review_score"]
        one = order_reviews[order_reviews == 1].sum()
        two = order_reviews[order_reviews == 2].sum()
        three = order_reviews[order_reviews == 3].sum()
        four = order_reviews[order_reviews == 4].sum()
        five = order_reviews[order_reviews == 5].sum()
        merged = [one, two, three, four, five]      
        return merged
    
    # 5.2. Score versus seller 
    # - insight: bei welchen Verkäufern sind Kunden besonders unzufrieden?
    # @author: Robin Schumacher
    def getReviewScoreVersusSeller(self):
        order_reviews = self.dataAccessor.getTable("reviews")[["order_id", "review_score"]]
        seller_id = self.dataAccessor.getTable("order_items")[["order_id", "seller_id"]]
        merge = pd.merge(left = seller_id, right = order_reviews, how='left', on = ["order_id"]).drop(columns= ["order_id"]).dropna()
        merge = merge.groupby(by = "seller_id").mean("review_score")
        return merge
    
    # 5.3. Score versus product
    # - insight: bei welchen Produkten` sind Kunden besonders unzufrieden?
    # - erstellung: robin
    def getReviewScoreVersusProduct(self):
        order_reviews = self.dataAccessor.getTable("reviews")[["order_id", "review_score"]]
        product_id = self.dataAccessor.getTable("order_items")[["order_id", "product_id"]]
        merge = pd.merge(left = product_id, right = order_reviews, how='left', on = ["order_id"]).drop(columns= ["order_id"]).dropna()
        merge = merge.groupby(by = "product_id").mean("review_score")
        return merge
    
    # 5.4. Score versus delivery time
    # - insight: wie stark ist die delivery time  auf die customer satisfaction
    # @author: Robin Schumacher
    def getReviewVersusVersusDeliveryTime(self):
        order_reviews = self.dataAccessor.getTable("reviews")[["order_id", "review_score"]]
        delivery_time = self.dataAccessor.getTable("orders")[["order_id", "order_status", "order_purchase_timestamp", "order_delivered_customer_date"]]
        delivery_time = delivery_time[delivery_time.order_status == "delivered"].drop(columns= ["order_status"])
        delivery_time["difftime"] = (pd.to_datetime(delivery_time["order_delivered_customer_date"]).dt.date - pd.to_datetime(delivery_time["order_purchase_timestamp"]).dt.date)
        delivery_time["difftime"] = delivery_time["difftime"].astype('timedelta64[D]')
        merge = pd.merge(left = delivery_time, right = order_reviews, how='left', on = ["order_id"]).drop(columns= ["order_id"]).dropna()
        merge = merge.groupby(by = "difftime").mean("review_score")
        return merge


class Plotter:
    
    def __init__(self):
        pass
    
    def plotMap(self, extend, title1, title2, datalon, datalat):
        fig, ax = plt.subplots(subplot_kw={"projection":ccrs.PlateCarree()}, figsize=(20,15))
        ax = plt.axes(projection=ccrs.PlateCarree())
        ax.set_extent(extend, ccrs.PlateCarree())
        ax.coastlines(resolution='110m')
        ax.add_feature(cartopy.feature.BORDERS)
        ax.add_feature(cartopy.feature.RIVERS)
        plt.scatter(datalon, datalat, s=1)
        ax.set_title(title2, fontsize=30)
        fig.suptitle(title1, fontsize=40)

    def plotLine(self, data, xleft, xright, xlabel, ylabel, title):
        plt.figure(figsize=(20,15))
        plt.plot(data)
        plt.xlim(xleft,xright)
        plt.title(title, fontsize=40)
        plt.gca().set_xlabel(xlabel, fontsize=30)
        plt.gca().set_ylabel(ylabel, fontsize=30)
        plt.gca().tick_params(labelsize=30)


        # @author: Robin Schumacher  
    def plotScatter(self, y, x, xlabel, ylabel, title):
        plt.figure(figsize=(20,15))
        plt.scatter(y, x)
        plt.title(title, fontsize=40)
        plt.gca().set_xlabel(xlabel, fontsize=30)
        plt.gca().set_ylabel(ylabel, fontsize=30)
        plt.gca().tick_params(labelsize=30)

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
        plt.figure(figsize=(20,15))
        plt.gca().pie(sizes, explode=explore, labels=labels, autopct='%1.1f%%', shadow=True, startangle=90, textprops={"fontsize":30})
        plt.title(title, fontsize=40)
        plt.legend(pie[0],labels, bbox_to_anchor=(1,0.5), loc="center right", fontsize=10, bbox_transform=plt.gcf().transFigure)
        plt.gca().tick_params(labelsize=30)
        fig.suptitle(title, fontsize=40)

        
    def plotHist(self, data, bins, title):
        plt.figure()
        plt.hist(data, bins=bins)
        plt.title(title)
        
    def plotBoxplot(self, data, xlabel, ylabel, title):
        plt.figure()
        plt.gca().set_xlabel(xlabel)
        plt.gca().set_ylabel(ylabel)
        plt.title(title)
        plt.boxplot(data)
        
         
        
class OutputManager:
    def __init__(self, reportId):
        self.outputfolderPlot = "output/data/plots/" + reportId
        self.outputfolderText = "output/data/text/" + reportId
        self.dataAccessor = DataAccessor()
    def saveFig(self, figurename):
        figure=plt.gcf()
        figure.savefig(self.outputfolderPlot + "/" + figurename + ".png")
        
class HashToIntConverter:
    def __init__(self, hashvalues):
        self.hashvalues = hashvalues
    def getDict(self):
        hashvalues = self.hashvalues
        result={}
        for index, hashvalue in enumerate(hashvalues):
            result[hashvalue] = index
        return result
    def convert(self, data):
        dicttemp = self.getDict()
        indexlist = []
        for index in data.index:
            indexlist.append(dicttemp[index])
        data.index = indexlist
        return data



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
        self.outputManager.saveFig("1.1 geoLocationCustomer")
        
        # 1.2 Plot price distribution
        # --> insight: Einordnung Güter/Preise                       
        priceDistribution = self.dataWrangler.getOrderPriceDistribution()
        self.plotter.plotHistInRange(priceDistribution, 1000, 0, 1000, "Price (Brazilian Real Equals ~ 0.16 EUR)", "Price Distribution of Orders")
        self.outputManager.saveFig("1.2 totalPriceDistributionOfOrders")
        
        # 1.3 orders per day/dayofweek/month/season/product (line) 
        #  --> insight: Nachfrageverteilung innerhalb des Jahres  
        # @author: Robin Schumacher                         
        order_time = self.dataWrangler.getGroupOrdernumbersByTimePeriod() 

        orders_day = order_time.groupby(by ='Day_order').count().drop(columns=['Season_order', 'Year_order', 'order_purchase_timestamp', 'product_id', 'Month_order', "Dayofweek_order"]).rename(columns={"order_id":"count_day"})
        divider = pd.to_datetime(order_time["order_purchase_timestamp"]).dt.date.drop_duplicates(keep = "first")
        divider = pd.to_datetime(divider).dt.day
        divider = divider.groupby(by = divider).count()
        orders_day["count_day"] = orders_day["count_day"] / divider
        self.plotter.plotLine(orders_day, 1, 31, "Day of Month", "Count", "Average Orders per Dayofmonth")
        self.outputManager.saveFig("1.3 Orders per Dayofmonth")

        orders_dayofweek = order_time.groupby(by = "Dayofweek_order").count().drop(columns=['Season_order', 'Year_order', 'order_purchase_timestamp', 'product_id', 'Month_order', "Day_order"]).rename(columns={"order_id":"count_dayofweek"})
        divider = pd.to_datetime(order_time["order_purchase_timestamp"]).dt.date.drop_duplicates(keep = "first")
        divider = pd.to_datetime(divider).dt.dayofweek
        divider = divider.groupby(by = divider).count()
        orders_dayofweek["count_dayofweek"] = orders_dayofweek["count_dayofweek"] / divider
        self.plotter.plotLine(orders_dayofweek, 0, 6, "Dayofweek", "Count", "Average Orders per Dayofweek")
        self.outputManager.saveFig("1.3 Orders per Dayofweek")

        orders_month = order_time.groupby(by ='Month_order').count().drop(columns=['Season_order', 'Year_order', 'order_purchase_timestamp', 'product_id', 'Day_order', "Dayofweek_order"]).rename(columns={"order_id":"count_month"})
        divider = pd.to_datetime(order_time["order_purchase_timestamp"]).dt.date.drop_duplicates(keep = "first")
        divider = pd.to_datetime(divider).dt.month
        divider = divider.groupby(by = divider).count()
        orders_dayofweek["count_month"] = orders_month["count_month"] / divider
        self.plotter.plotLine(orders_month, 1, 12, "Month", "Count", "Average Orders per Month")
        self.outputManager.saveFig("1.3 Orders per Month")

        orders_season = order_time.groupby(by ='Season_order').count().drop(columns=['Month_order', 'Year_order', 'order_purchase_timestamp', 'product_id', 'Day_order', "Dayofweek_order"]).rename(columns={"order_id":"count_season"})
        #number of seasons to divide
        divider = [4,2,2,3]
        orders_season["count_season"] = (orders_season["count_season"] / divider)
        self.plotter.plotLine(orders_season, 1, 4, "Season", "Count", "Average Orders per Season")
        self.outputManager.saveFig("1.3 Orders per Season")

        orders_year = order_time.groupby(by ='Year_order').count().drop(columns=['Month_order', 'Season_order', 'order_purchase_timestamp', 'product_id','Day_order', "Dayofweek_order"]).rename(columns={"order_id":"count_year"})
        self.plotter.plotLine(orders_year, 2016, 2018, "Year", "Count", "Orders per Year")
        self.outputManager.saveFig("1.3 Orders per Year")
        
        orders_product = order_time.groupby(by ='product_id').count().drop(columns=['Month_order', 'Year_order', 'Season_order', 'order_purchase_timestamp', 'Day_order', "Dayofweek_order"]).rename(columns={"order_id":"count_product"})
        hashToIntConverter = HashToIntConverter(orders_product.index)
        orders_product = hashToIntConverter.convert(orders_product)
        self.plotter.plotLine(orders_product, 0, 1000, "Product ID", "Count", "Orders per Product")
        self.outputManager.saveFig("1.3 Orders per Product")

        # 1.4 plot number of sellers, customers and orders
        # --> Angebots- und Nachfragegröße    -> Textfile                        
        number_sellers = self.dataWrangler.getNumberSellers()                   
        number_customers = self.dataWrangler.getNumberOfCustomers()
        number_orders = self.dataWrangler.getNumberOfOrders()
        numbers = [number_sellers, number_customers, number_orders]
        self.plotter.plotBar(["sellers", "customer", "orders"], numbers, "",  "Total number sellers, customers, orders")
        self.outputManager.saveFig("1.4 totalNumberSellersCustomersOrders")

        # 1.5 Plots sales by sellers
        salesbyseller = self.dataWrangler.getSalesBySeller()
        #get the data
        # author: Ben Alexy
        hashToIntConverter15 = HashToIntConverter(salesbyseller.index)
        salesbyseller = hashToIntConverter15.convert(salesbyseller)
        #unhash (index) the sellerid
        #plot
        self.plotter.plotLine(salesbyseller, 10, 3300, "sellerid", "sales", "salesbyseller")
        self.outputManager.saveFig("1.5 salesbyseller")
        
        #plotHistInRange(self, data, bins, xleft, xright, xlabel, title):
        # 1.6 Plot sellers versus Product Category
        # --> Clustern der Verkäufer nach Produktkategorie zur Verkäuferanalyse
        # author: Ben Alexy                        
        sellerversusproductcategory = self.dataWrangler.getSellersVersusProductCatogory()
        #get data
        hashToIntConverter = HashToIntConverter(sellerversusproductcategory.index)
        
        sellerversusproductcategory = hashToIntConverter.convert(sellerversusproductcategory)
        #unhash (index) the id
        #plot it
        self.plotter.plotLine(sellerversusproductcategory, 10, 3300, "Seller ID", "Categorie", "Seller_idvscategory")
        self.outputManager.saveFig("1.6 getsellersvsproductcategory")
        
        # 1.8 Compute sellers versus product price (line)
        # --> insights: welche sellers verkaufen besonders hochpreisig? 
        #               Wie ist der Vergleich bei gleichen Produkten 
        #               und unterschiedlichen Verkäufern?   
        # @author: Robin Schumacher                  
        sellersvsproductprices = self.dataWrangler.getSellersVersusProductPrices()
        hashToIntConverter = HashToIntConverter(sellersvsproductprices.index)
        sellersvsproductprices = hashToIntConverter.convert(sellersvsproductprices)
        self.plotter.plotLine(sellersvsproductprices, 0, 100, "Seller ID", "Average Pricedifference (%)", "Sellers versus Productprices")
        self.outputManager.saveFig("1.8 Sellers versus Productprices")
    
    def printName(self):
        print("+++++ " + self.name + " ++++")
    
    
class Report_2_Business_Development:
    
    def __init__(self, dataWrangler, plotter):
        self.dataWrangler = dataWrangler
        self.plotter = plotter
        self.outputManager = OutputManager("report_2")
        self.name = "Report 2 Buiness Development"
        
    def execute(self):
        pass
        # 2.1 - plot gesamtumsatz und Gesamtumsatz abzüglich nicht zustande
        #       gekommene Kaufverträge (Lieferengpässe etc.) (barplot)
        # --> insight: Wie groß ist der Gesamtumsatz? Firmeneinordnung etc.
        # @author: Robin Schumacher
        totalprice = self.dataWrangler.getTotalPrice()
        self.plotter.plotBar(["Sales combined", "Cancelled Sales", "Actual Sales"], totalprice, "Sales (10 m. Real)",  "Combinded Sales")
        self.outputManager.saveFig("2.1 Saledifference")
        
        # 2.2 - plot Umsatz pro day/dayofweek/month/season/year/product (line)
        # --> insight: Budgets, Planung, Buchhaltung, Strategieentwicklung etc.
        # @author: Robin Schumacher
        order_time = self.dataWrangler.getMergeOrderPriceOrderOrderedDelivered()

        sales_day = order_time.groupby(by ='Day_order').sum("price").drop(columns=['Season_order', 'Year_order', 'Month_order', "Dayofweek_order"])
        divider = pd.to_datetime(order_time["order_purchase_timestamp"]).dt.date.drop_duplicates(keep = "first")
        divider = pd.to_datetime(divider).dt.day
        divider = divider.groupby(by = divider).count()
        sales_day["price"] = sales_day["price"] / divider
        self.plotter.plotLine(sales_day, 1, 31, "Dayofmonth", "Average Turnover (Real)", "Sales per Dayofmonth")
        self.outputManager.saveFig("2.2 Sales per Dayofmonth")

        sales_dayofweek = order_time.groupby(by = "Dayofweek_order").sum("price").drop(columns=['Season_order', 'Year_order', 'Month_order', "Day_order"])
        divider = pd.to_datetime(order_time["order_purchase_timestamp"]).dt.date.drop_duplicates(keep = "first")
        divider = pd.to_datetime(divider).dt.dayofweek
        divider = divider.groupby(by = divider).count()
        sales_dayofweek["price"] = sales_dayofweek["price"] / divider
        self.plotter.plotLine(sales_dayofweek, 0, 6, "Dayofweek", "Average Turnover (Real)", "Sales per Dayofweek")
        self.outputManager.saveFig("2.2 Sales per Dayofweek")

        sales_month = order_time.groupby(by ='Month_order').sum("price").drop(columns=['Season_order', 'Year_order', 'Dayofweek_order', "Day_order"])
        divider = pd.to_datetime(order_time["order_purchase_timestamp"]).dt.date.drop_duplicates(keep = "first")
        divider = pd.to_datetime(divider).dt.month
        divider = divider.groupby(by = divider).count()
        sales_dayofweek["price"] = sales_month["price"] / divider
        self.plotter.plotLine(sales_month, 1, 12, "Month", "Average Turnover (m. Real)", "Sales per Month")
        self.outputManager.saveFig("2.2 Sales per Month")

        sales_season = order_time.groupby(by ='Season_order').sum("price").drop(columns=['Dayofweek_order', 'Year_order', 'Month_order', "Day_order"])
        #number of seasons to divide
        divider = [4,2,2,3]
        sales_season["price"] = (sales_season["price"] / divider)
        self.plotter.plotLine(sales_season, 1, 4, "Season", "Average Turnover (m. Real)",  "Sales per Season")
        self.outputManager.saveFig("2.2 Sales per Season")

        sales_year = order_time.groupby(by ='Year_order').sum("price").drop(columns=['Season_order', 'Dayofweek_order', 'Month_order', "Day_order"])
        self.plotter.plotLine(sales_year, 2016, 2018, "Year", "Turnover (m. Real)", "Sales per Year")
        self.outputManager.saveFig("2.2 Sales per Year")
        
        sales_product = order_time.groupby(by ='product_id').sum("price").drop(columns=['Season_order', 'Year_order', 'Month_order', "Day_order", "Dayofweek_order"])
        hashToIntConverter = HashToIntConverter(sales_product.index)
        orders_product = hashToIntConverter.convert(sales_product)
        self.plotter.plotLine(sales_product, 0, 1000, "Product", "Turnover (Real)",  "Sales per Product")
        self.outputManager.saveFig("2.2 Sales per Product")
        
        
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
        # @author: Hendrik Garken           
        data = self.dataWrangler.getOrderStatusDistribution()
        groups = data.groupby(by = "order_status").groups
        labels = []
        amounts = []
        for k,g in groups.items():
            labels.append(k)
            amounts.append(len(list(g)))
        plt.pie(amounts, labels=labels)
        self.outputManager.saveFig("3.1 Order Status destribution")
        
        # 3.2 sellerid versus bad status
        # --> insight: Welche Verkäufer sind besonder schlecht beim Zustellen?
        # @author: Hendrik Garken      
        seller_bad_status = self.dataWrangler.getSellerWithBadOrderStatus()
        self.plotter.plotBoxplot(seller_bad_status, "sellers", "time difference in hours", "3.2 Sellers with bad order status")
        self.outputManager.saveFig("3.2 boxplot")
               
        # 3.3 amount of delivery delay much worse than expected (barplot)
        # -->  insight: wie sieht die Gesamtlage der Vorhersage aus
        # @author: Robin Schumacher
        prediction = self.dataWrangler.getAmountOfWrongDeliveryPredictions()
        self.plotter.plotBar(["Faster", "Same Day", "1-2", "3-5", "6-10", "10-20", "20-50", "50-x"], prediction, "Days Delivery later than prediction",  "Difference prediction to arrival")
        self.outputManager.saveFig("3.3 Delivery Delay")
        
        # 3.4 paket size versus delivery date 
        # --> insight: inwiefern hat die Größe des Pakets einen Einfluss auf die Zulieferzeit?
        # @author: Jakob Poley
        packegesize = self.dataWrangler.getInfluenceOfPaketSizeOnDeliveryTime()
        self.plotter.plotScatter(packegesize["deliverytime"], packegesize["volume_l"], "deliverytime", "packagesize", "Influence of packet size on deliverytime")
        self.outputManager.saveFig("3.4 Influence Of Packet Size On Deliverytime")

        # 3.5 delivery data delay versus geo data 
        # --> insight: in welchen Zulieferorten ist die Zustellung besonders schlecht? -> Optimierungsbedarf
        # @author: Jakob Poley
        meanDelaeyPerState = self.dataWrangler.getDeliveryDateDelayVersusGeoLocation()
        statenames = ["AC","AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"] 
        self.plotter.plotBar(statenames, meanDelaeyPerState, "",  "Mean Delay Per State")
        self.outputManager.saveFig("3.5 Mean Delay per State")
        
    def printName(self):
        print("+++++ " + self.name + " ++++")
    
    
class Report_4_Customer_Behaviour:
    
    def __init__(self, dataWrangler, plotter):
        self.dataWrangler = dataWrangler
        self.plotter = plotter
        self.outputManager = OutputManager("report_4")
        self.name = "Report 4 Customer Behaviour"
        
    def execute(self):
        # 4.1 Number payments per Paymenttypes (barplot)
        # --> insight: Welche Bzahlart ist am beliebtesten
        # @author: Robin Schumacher
        payment = self.dataWrangler.getNumberOfPaymenttypes()
        self.plotter.plotBar(["boleto", "credit_card", "debit_card", "not_defined", "voucher"], payment, "Count",  "Usage of Paymenttypes")
        self.outputManager.saveFig("4.1 Most used Paymenttypes")

        # 4.2. ReviewAnswerSpeed (hist)
        # --> insight: wie schnell kann mit review Antworten gerechnet werden?
        # @author: Robin Schumacher
        answer_time = self.dataWrangler.getReviewAnswerSpeed()
        self.plotter.plotHistInRange(answer_time, 1000, 0, 400, "Answer Time [h]", "Review answer Speed")
        self.outputManager.saveFig("4.2 Review answer speed")
        
        # 4.3 PaymenttypeüberPreis (scatter plot)   
        # --> insight: Welche Bezahlart wird bei welchem Einkaufswert am häufigsten genutzt
        # @author: Robin Schumacher
        payment = self.dataWrangler.getPaymenttypeüberPreis()
        paymenttype = payment["payment_type"]
        paymentvalue = payment["payment_value"]
        self.plotter.plotScatter(paymenttype, paymentvalue, "Type of Payment", "Value", "Used Paymenttype by Price")
        self.outputManager.saveFig("4.3 Paymenttype over Price")


    def printName(self):
        print("+++++ " + self.name + " ++++")
   
    
class Report_5_Customer_Satisfaction:
    
    def __init__(self, dataWrangler, plotter):
        self.dataWrangler = dataWrangler
        self.plotter = plotter
        self.outputManager = OutputManager("report_5")
        self.name = "Report 5 Customer Satisfaction"
        
    def execute(self):    
        # 5.1. score distribution (barplot) 
        # - insight: wie ist die Gesamtzufriedenheit einzuschätzen?
        # @author: Robin Schumacher
        score_distribution = self.dataWrangler.getGeneralCustomerSatisfaction()
        self.plotter.plotBar(["1", "2", "3", "4", "5"], score_distribution, "Review Score",  "Count of Score at Reviews")
        self.outputManager.saveFig("5.1 Score Distribution")
        
        # 5.2. Score versus seller (line) 
        # - insight: bei welchen Verkäufern sind Kunden besonders unzufrieden?
        # @author: Robin Schumacher
        seller_score = self.dataWrangler.getReviewScoreVersusSeller()
        hashToIntConverter = HashToIntConverter(seller_score.index)
        seller_score = hashToIntConverter.convert(seller_score)
        self.plotter.plotLine(seller_score, 0, 100, "Seller ID", "Review Score", "Score per Seller")
        self.outputManager.saveFig("5.2 Score versus Seller")

        # 5.3. Score versus product (line) 
        #- insight: bei welchen Produkten` sind Kunden besonders unzufrieden?
        # @author: Robin Schumacher
        product_score = self.dataWrangler.getReviewScoreVersusProduct()
        hashToIntConverter = HashToIntConverter(product_score.index)
        seller_score = hashToIntConverter.convert(product_score)
        self.plotter.plotLine(product_score, 0, 100, "Product ID", "Review Score", "Score per Product")
        self.outputManager.saveFig("5.3 Score versus Product")
       
        # 5.4. Score versus delivery time (line)
        # insight: wie stark ist die delivery time  auf die customer satisfaction
        # @author: Robin Schumacher
        delivery_time = self.dataWrangler.getReviewVersusVersusDeliveryTime()
        self.plotter.plotLine(delivery_time, 0, 210, "Time difference in [D]", "Review Score", "Score per Deliverytime")
        self.outputManager.saveFig("5.4 Score versus Deliverytime")
    
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

   processor = ProcessorManually([report1])
   processor.execute()

