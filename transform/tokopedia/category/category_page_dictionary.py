# -*- coding: utf-8 -*-
"""
Created on Fri Oct 21 14:39:08 2022

@author: SIRCLO
"""
from transform.tokopedia.category.processing import Processing

class Category:

    def __init__(self):
        p = Processing()

        self.html_code_page = {
            "l2_category": ["css-1t4ww55", "h1"],
            "l1_category": ["css-wms9d7", "a", 2],
            "l3_category": ["css-wms9d7", "a", 3]
            }

        self.html_code_page_item = {
            "product_name": [
                "css-20kt3o",
                "span"
            ],
            "final_price": [
                "css-o5uqvq",
                "span"
            ],
            "store_name": [
                "css-ywdpwd",
                "span"
                , 1],
            "product_url": [
                "css-54k5sq",
                "a"
            ],
            "store_id": [
                "css-54k5sq",
                "a"
                , 0],
            "product_id": [
                "css-54k5sq",
                "a"
                , 1],
            "location": [
                "css-ywdpwd",
                "span"
                , 0],
            "original_price": [
                "css-rn1hus",
                "div"
            ],
            "sold":[
                "css-1riykrk", "div"
            ],
            "store_type":[
                "css-1hy7m5k", "div"
            ]

        }

        self.df_category = {

            "product_id": [],
            "product_name": [],
            "final_price": [],
            "original_price": [],
            "product_url": [],
            "store_id": [],
            "store_name": [],
            "location": [],
            "l3_category": [],
            "l2_category": [],
            "l1_category": [],
            "sold": [],
            "store_type": []
        }

        self.df_formula = {
            "product_name": p.find_string,
            "final_price": p.find_integer,
            "store_name": p.find_all_index,
            "location": p.find_all_index,
            "product_url": p.find_url,
            "store_id": p.url_to_id,
            "product_id": p.url_to_id,
            "original_price": p.check_sale,
            "l2_category": p.find_string,
            "l3_category": p.find_all_index,
            "l1_category": p.find_all_index,
            "sold": p.find_integer,
            "store_type": self.find_store_type_url
        }

    def find_store_type_url(self, html_item, class_, tag, tag_identifier="class"):
        value = html_item.find(tag, {tag_identifier:class_})
        value = value.find("img", alt=True)["src"]
        return value
