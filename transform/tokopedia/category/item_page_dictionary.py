# -*- coding: utf-8 -*-
"""
Created on Thu Oct 27 10:56:57 2022

@author: SIRCLO
"""
from transform.tokopedia.processing import Processing


class Item:

    def __init__(self):
        self.p = Processing()
        self.html_code_page_item = {
            "url":[
                "branch:deeplink:$desktop_url", "meta", "name"
            ],
            "product_id": [
                 "branch:deeplink:$ios_deeplink_path","meta", "name"
            ],
            "store_id": [
                "css-1nf4dbc", "img", "src"
            ],
            "sold": [
                "lblPDPDetailProductSoldCounter",
                "div", "data-testid"
            ],
            "stock": [
                "css-1hhh2ha-unf-heading e1qvo2ff8",
                "p"
            ],
            "seller_type": [
                "css-ebxddb",
                "img", "alt"],

            "item_rating": [
                "lblPDPDetailProductRatingNumber",
                "span", "data-testid", 1
            ],
            "rating_count": [
                "lblPDPDetailProductRatingCounter",
                "span", "data-testid"
            ],

            "discussion_count": [
                "lblPDPDetailProductDiscussionNumber",
                "div", "data-testid"
            ],
            "response_time": [
                "lblPDPShopPackSecond",
                "div", "data-testid"
            ],
            "item_image": [
                "PDPMainImage", "img", "src", "data-testid"
            ],
            "item_desc": [
                "lblPDPDescriptionProduk",
                "div", "data-testid"
            ]

        }

        self.df_category = {
            "url": [],
            "product_id": [],
            "store_id": [],
            "sold": [],
            "stock": [],
            "seller_type": [],
            "item_rating": [],
            "rating_count": [],
            "discussion_count": [],
            "response_time": [],
            "item_image": [],
            "item_desc": []
        }

        self.df_formula = {
            "url": self.p.find_meta_content,
            "store_id": self.find_store_id,
            "product_id": self.find_product_id,
            "sold": self.find_sold,
            "stock": self.p.find_integer,
            "seller_type": self.p.find_image,
            "response_time": self.p.find_integer,
            "item_rating": self.p.find_integer,
            "rating_count": self.p.find_integer,
            "discussion_count": self.p.find_integer,
            "item_image": self.p.find_image,
            "item_desc": self.p.find_string
        }

    def find_sold(self, html_item, class_, tag, tag_identifier="class"):
        value = self.p.find_string(html_item, class_, tag, tag_identifier)
        value = value.replace(' rb', '000')
        value = self.p.string_to_integer(value)
        return value

    def find_product_id(self, html_item, class_, tag, tag_identifier="class"):
        value = self.p.find_meta_content(html_item, class_, tag, tag_identifier)
        value = value.rsplit('/', 2)[-1]
        return value

    def find_store_id(self, html_item, class_, tag, img_element, tag_identifier="class"):
        item = self.p.find_image(html_item, class_, tag, img_element, tag_identifier)
        split_item = item.split("/")
        value = split_item[10]
        return value