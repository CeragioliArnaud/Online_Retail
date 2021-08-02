# -*- coding: utf-8 -*-
"""
Created on Mon Aug  2 00:44:24 2021

@author: arnaud.ceragioli
"""
from tqdm import tqdm

class FoundValuesTester:
    
    def testItFirst(self, resultMongo, dataframe):
    
        try:
            for res in tqdm(resultMongo):
                assert res["numberOfElements"] == len(dataframe.get_group(res["_id"]))
            return True
        
        except:
            return False
        
    
    def testItSecond(self, resultMongo, score, name):
    
        try:
            assert score == resultMongo["sumBoughtItems"]
            assert name == resultMongo["_id"]
            return True
        
        except:
            return False
        
    
    def testItThird(self, resultMongo, score, name):
    
        try:
            assert score == resultMongo["moneySpent"]
            assert name == resultMongo["_id"]
            return True
        
        except:
            return False
        
        
    def testItFourth(self, resultMongo, list_of_values):

        try:
            assert len(resultMongo) == len(list_of_values)
            
            for i in tqdm(range(0, len(resultMongo))):
                assert resultMongo[i]["_id"][0] == list_of_values[i]["_id"][0] and resultMongo[i]["_id"][1] == list_of_values[i]["_id"][1] or resultMongo[i]["amountItems"] == list_of_values[i]["amountItems"]
            return True
        
        except:
            return False
    
    
    def testItFifth(self, resultMongo, dataframe):

        try:
            assert resultMongo["AverageValue"] == dataframe
            return True
        
        except:
            return False
        
    
    def testItSixth(self, resultMongo, list_of_values):

        try:
            assert len(resultMongo) == len(list_of_values)
            
            for i in tqdm(range(0, len(resultMongo))):
                assert resultMongo[i]["_id"][0] == list_of_values[i]["_id"][0] and resultMongo[i]["_id"][1] == list_of_values[i]["_id"][1] or (round(resultMongo[i]["priceDistribution"], 2) == round(list_of_values[i]["priceDistribution"], 2)) or ((round(resultMongo[i]["priceDistribution"], 2) - round(list_of_values[i]["priceDistribution"], 2) <= 0.1) and (round(resultMongo[i]["priceDistribution"], 2) - round(list_of_values[i]["priceDistribution"], 2) >= -0.1))
            return True
        
        except:
            return False
        
        
    def testItSeventh(self, resultMongo, list_of_values):

        try:
            assert len(resultMongo) == len(list_of_values)
            
            for i in tqdm(range(0, len(resultMongo))):
                assert resultMongo[i]["_id"] == list_of_values[i]["_id"] or (round(resultMongo[i]["RatioUnit"], 2) == round(list_of_values[i]["RatioUnit"], 2)) or ((round(resultMongo[i]["RatioUnit"], 2) - round(list_of_values[i]["RatioUnit"], 2) <= 0.1) and (round(resultMongo[i]["RatioUnit"], 2) - round(list_of_values[i]["RatioUnit"], 2) >= -0.1))
            return True
        
        except:
            return False
        
    
    def testItEigth(self, resultMongo, list_of_values):
    
        try:
            assert len(resultMongo) == len(list_of_values)
            
            for i in tqdm(range(0, len(resultMongo))):
                assert resultMongo[i]["_id"] == list_of_values[i]["_id"]
                assert resultMongo[i]["transactionsPerCountry"] == list_of_values[i]["transactionsPerCountry"]
            return True
        
        except:
            return False