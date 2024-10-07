
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 20 11:04:40 2020

@author: a
"""

import pandas as pd
from matplotlib import pyplot as plt

orders = pd.read_csv("../../../data/olist_orders_dataset.csv")
orderitems= pd.read_csv("../../../data/olist_order_items_dataset.csv")
customers = pd.read_csv("../../../data/olist_customers_dataset.csv")
sellers = pd.read_csv("../../../data/olist_sellers_dataset.csv")
reviews = pd.read_csv("../../../data/olist_order_reviews_dataset.csv")


data = customers.merge(orders, on="customer_id", how="inner").merge(
    orderitems, on="order_id", how="inner").merge(reviews, on="order_id", how="inner")

#print(customers.merge(orders, on="customer_id", how="inner").merge(
#    orderitems, on="order_id", how="inner").merge(reviews, on="order_id", how="inner").columns)
#print(data.iloc[0])
#print(data.dtypes)
data["order_purchase_timestamp"]=pd.to_datetime(data["order_purchase_timestamp"])
data["order_delivered_customer_date"]=pd.to_datetime(data["order_delivered_customer_date"])
data["order_estimated_delivery_date"]=pd.to_datetime(data["order_estimated_delivery_date"])

######## 1.
# 1 different review scores:
def plotRevueScore():
    plt.hist(data["review_score"])
    plt.gca().set_xlabel("score level")
    plt.gca().set_ylabel("amount A.U.")
    plt.title("review score distribution")

######### 2.
# bewertungen

def plotSellerVersusScore(data, score):
    data = data.loc[data["review_score"]==score]
    evaldata = data.groupby("seller_id")["customer_id"].count().sort_values(ascending=True)
    evaldata.index=evaldata.astype("category").cat.codes
    return evaldata
    
if __name__=="__main__":
    plt.figure()
    for score in range(1,6):
        evaldata = plotSellerVersusScore(data, score)
        plt.plot(evaldata, label="score " + str(score))
    plt.legend()
    plt.title("Scoring versus seller distribution")
    plt.gca().set_xlabel("arbitrary seller_id")
    plt.gca().set_ylabel("amount of reviews (#)")
 