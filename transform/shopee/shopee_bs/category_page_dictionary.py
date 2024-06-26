from transform.shopee.shopee_bs.processing import Processing
	
class Dictionary:

    def __init__(self):
        p = Processing()

        self.css_item_config = {
             "name": "div[class='_44qnta'] span", 
             "jum_penilaian": "div[class='_1k47d8']", 
             "terjual": ".P3CdcB",
             "favorit": "button[class='IYjGwk'] div[class='Ne7dEf']",
             "current_price": ".pqTWkA", 
             "price_before_disc": ".Y3DvsN", 
             "disc_percent": "._0voski", 
             "nama_os": ".VlDReK"
        }
        
        self.xpath_item_config = {
            "stok": "//div[contains(text(),'tersisa')]"
        }

        self.div_find_item_config = {
            "terjual_detail": "Pbtfc9 rvQll2" , 
            "icon_terjual": "(//*[name()='svg'][@class='stardust-icon stardust-icon-help _0WSdLi'])[1]", 
            "toko_jum_produk": "Xkm22X vUG3KX"
        }

        self.div_findall_item_config = {
            "spesifikasi": "dR8kXc",
            "toko_penilaian": "R7Q8ES _07yPll"
        }


        self.xpath_class_category_config = {
            "name":["_1yN94N WoKSjC _2KkMCe"],
            "kota_penjual":["mrz-bA"],
            "terjual":["x+3B8m wOebCz"],
            "promo1":["_1FKkT _3Ao0A"],
            "promo2":["_6F-9FS"],
            "price_before_discount":["cbl0HO _90eCxb It3cSY"], 
            "bonus1":["A4rAzg"], 
            "bonus2":["mfd--s"],
            "os1_page": ["tvY3nR"], 
            "os2_page": ["iyNA1I"]
        }

        self.df_item = { "name":[],
                        "kota_penjual":[],
                        "terjual":[],
                        "promo1":[],
                        "promo2":[],
                        "price_before_discount":[], 
                        "bonus1": [], 
                        "bonus2":[], 
                        "os1_page":[],
                        "os2_page":[]
                        }
        
        self.class_category_config = { "name":"TxwJWV _2qhlJo rrh06d",
                                        "kota_penjual":"RGH3sw",
                                        "terjual":"vbHrXG Rd0GDT" ,
                                        "promo1":"_1FKkT _3Ao0A" ,
                                        "promo2":"e0Wnzn",
                                        "price_before_discount":"B+I1a7 chadah Mi4ApM", 
                                        "bonus1":"KlWH64" , 
                                        "bonus2":"IlLkyY" 
                                        }

        self.os_type_xpath = { 
            "os1_item": "//div[@class='+bpQZ1 Z8rK9d vibfFd Og9k1m R89n5h items-center']", 
            "os2_item": "//div[@class='YPqix5']//*[name()='svg']//*[name()='path']", 
        }
        
        self.list_col_toint_category = ["sameAs", "productID", "aggregateRating_ratingCount"]

        self.list_col_tofloat_category = ["offers_lowPrice", "offers_highPrice", "aggregateRating_bestRating",
                        "aggregateRating_worstRating", "aggregateRating_ratingValue", "offers_price"]

        self.to_drop_col_category = ['@context', '@type', 'potentialAction.@type', 'potentialAction.target',
                   'potentialAction.query-input', 'description', 'itemListElement']

        self.col_df_item = ["name", "jum_penilaian", "terjual", "favorit", "current_price", "price_before_disc", 
                                "disc_percent", "stok", "dikirim_dari", "spesifikasi", "run_date",
                                "toko_jum_penilaian", "toko_jum_produk","toko_persen_dbalas", "toko_wkt_dbalas", "toko_bergabung",
                                "toko_pengikut", "product_id", "url", "shop_id", "nama_os", "os_type", "additional_info", "terjual_detail", "category"]