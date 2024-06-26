# -*- coding: utf-8 -*-
"""
Created on Thu Oct 27 10:53:13 2022

@author: SIRCLO
"""
import re
from urllib.parse import unquote

class Processing:
    def __init__(self):
        pass
    
    def string_to_integer(self,value):
        value = int(re.sub("[^0-9]", "", value))
        return value
    
    def find_string(self, html_item, class_, tag, tag_identifier="class"):
        value = html_item.find(tag, {tag_identifier: class_}).get_text()
        return value 
    
    def find_all(self, html_item, class_, tag,tag_identifier="class"):
        list_value = html_item.findAll(tag, {tag_identifier: class_})
        list_value = [item.get_text() for item in list_value]
        return list_value
    
    def find_all_index(self, html_item, class_, tag, index):
        value = self.find_all(html_item, class_, tag)[index]
        return value
    
    def find_integer(self, html_item, class_, tag, tag_identifier="class",float_=0):
        value = self.find_string(html_item, class_, tag, tag_identifier)
        if float_ == 0:
            value = self.string_to_integer(value)
        else:
            value = float(value)
        return value
    
    def remove_until_string(self,string,string_stop):
        s = re.sub('.*'+string_stop, '', string)
        return s
    
    def find_url(self, html_item, class_, tag,tag_identifier="class"):
        value = html_item.find(tag, {tag_identifier:class_}, href=True)['href']
        value = self.remove_until_string(value, 'r=')
        value = re.sub('%3F.*', '', value)
        value = unquote(value)
        return value
    
    def find_image(self, html_item, class_, tag, img_element, tag_identifier="class"):
        value = html_item.find(tag, {tag_identifier:class_}, alt=True)[img_element]
        value = self.remove_until_string(value, 'r=')
        value = re.sub('%3F.*', '', value)
        value = unquote(value)
        return value
    
    def url_to_id(self, html_item, class_, tag, index):
        value = self.find_url(html_item, class_, tag)
        splitted_url = self.remove_until_string(value,'https://www.tokopedia.com/').split('/')
        value = splitted_url[index]
        return value
    
    def check_sale(self, html_item, class_, tag):
        try:
            value = self.find_string(html_item, class_, tag)
            value = self.remove_until_string(value,"Rp")
            value = self.string_to_integer(value)
        except:
            value = 0
        return value

    def find_meta_content(self, html_item, class_, tag, tag_identifier="class"):
        value = html_item.find(tag, {tag_identifier: class_})
        value = value["content"]
        return value