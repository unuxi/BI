# -*- coding: utf-8 -*-
"""
Created on Thu Jan  7 14:38:06 2021

@author: Hendrik
"""

import time 
import datetime 
  
   
print(time.mktime(datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S").timetuple()))