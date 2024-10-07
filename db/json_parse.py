#!usr/bin/env python3
# -*- coding: utf-8 -*-

"""
parse JSON configuration file

@author: Jakob Poley
@institution: TH Lübeck
@date: 23. Januar 2021

"""
import json

# parses JSON config file
class JsonParser:

    # constructor that sets the JSON file path
    def __init__(self, jsonfilepath):
        self.jsonfilepath = jsonfilepath

    # parsing the JSON file
    def parse(self):
        with open(self.jsonfilepath) as json_file:
            data= json.load(json_file)
        return data
