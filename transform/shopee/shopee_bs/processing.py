from helpers.cloud_storage_helper import list_blob_gcs, download_blob_to_local
import json
import pandas as pd

class Processing:
    def __init__(self):
        pass

    def download_from_gcs(self, category: str, base_dir: str, base_path: str, bucket_name: str, run_date: str, get_prev_date="0", prev_date="0") :
        
        page_type = base_path.split('/')[-1]
        if int(get_prev_date) == 1 : 
            list_blob = list_blob_gcs(bucket_name, f"{base_path}/{prev_date}")
        else : 
            list_blob = list_blob_gcs(bucket_name, f"{base_path}/{run_date}/{category}")

        # only for case get previous date item
        for index, file in enumerate(list_blob):
            print(f"list file in blob gcs: {file}")
            if page_type == 'item' : 
                file_name = f'{base_dir}/{file.split("/")[5].split("_item")[0]}_item_{str(index)}.html'
            if page_type == 'category' : 
                file_name = f'{base_dir}/{file.split("/")[5].split("_page")[0]}_page_{str(index)}.html'
            try : 
                download_blob_to_local(bucket_name=bucket_name, local_file_name=file_name, gcs_blob_name=file)
                print(f'download finish for {file_name}')
            except : 
                print("file can't be downloaded")
                continue

    #function to recheck if not getting any data by css method
    def recheck_result_css(self, soup, var) : 
        result = soup.select(var)
        if len(result) == 0 : 
            return 'n/a'
        else : 
            return result[0].text
        
    def recheck_result_xpath(self, dom, var) : 
        result = dom.xpath(var)
        if len(result) == 0 : 
            return 'n/a'
        else : 
            return result[0].text
        
    #function to recheck if not getting any data by class method
    def recheck_result_class(self, var) : 
        if var is None: 
            return 'n/a'
        else : 
            return var
    
    #function to get 2 variable; spesifikasi & dikirim_dari
    def transform_spesifikasi(self, var) : 
        try : 
            spesifikasi = [item.text for item in var] 
            dikirim_dari = spesifikasi[-1].split('Dari')[1]   
        except : 
            spesifikasi = []
            dikirim_dari = 'n/a'
        
        spec = ' | '.join([str(item) for item in spesifikasi]),
        return [spec, dikirim_dari]
    
    #function to get 5 variable which is derivated from toko_penilaian value
    def transform_penilaian(self, var) : 
        toko_penilaian  = [item.get_text() for item in var]
        if len(toko_penilaian) > 0 : 
            toko_jum_penilaian = toko_penilaian[0]
            toko_persen_dbalas = toko_penilaian[1]
            toko_wkt_dbalas    = toko_penilaian[2]
            toko_bergabung     = toko_penilaian[3]
            toko_pengikut      = toko_penilaian[4]
        else : 
            toko_jum_penilaian = 'n/a'
            toko_persen_dbalas = 'n/a'
            toko_wkt_dbalas    = 'n/a'
            toko_bergabung     = 'n/a'
            toko_pengikut      = 'n/a'
        return [ toko_jum_penilaian, toko_persen_dbalas, toko_wkt_dbalas, toko_bergabung, toko_pengikut ]
    
    def split_app_json(self, dom) : 
        try : 
            app_json = self.recheck_result_class(dom.xpath('''(//script[contains(text(),'{"@context":"http:')])[2]''')[0].text)
            print(app_json)
            # product_id = app_json.split('"productID":"')[1].split('","image"')[0]
            url = app_json.split(',"url":')[1].split('","productID')[0]
            product_id = url.split('.')[-1]
            shop_id = url.split('.')[-2]
        except : 
            url = 'n/a'
            product_id= 'n/a'
            shop_id = 'n/a'

        return [product_id, url, shop_id,app_json]

    def get_os_type(self, dom, var_type_1, var_type_2) : 
        os_type_1 = self.recheck_result_xpath(dom=dom, var=var_type_1)
        os_type_2 = dom.xpath(var_type_2)
        if os_type_1 != 'n/a' : 
            os_type = os_type_1
        elif len(os_type_2) == 0 : 
            os_type = 'not star or mall'
        else : 
            os_type = 'Mall'
        return os_type

    def get_os_category(self, row) : 
        if len(row['os1_page']) > 0: 
            val = row['os1_page']
        elif row['os2_page'] == 'Mall' : 
            val = 'Mall'
        else : 
            val = 'not Star or Mall'
        return val
        
    def find_class_helper(self, soup ,var) : 
        value = soup.find('div', class_=var)
        if value is None : 
            result = 'n/a'
        else : 
            result = value.get_text()
        return result
    
    def find_div_class(self, soup, var) : 
        result = soup.find('div',class_=var).get_text()
        return result

    def normalize_json_html(self, var) : 
        all_data = {}
        x = 1
        for i in range(len(var)) : 
            for data in var[i] : 
                json_object = json.loads(data)
                all_data[f'df_{x}'] = pd.json_normalize(json_object)
                x = x + 1
        return all_data
    
    def find_class_helper(self, soup ,var) : 
        value = soup.find('div', class_=var)
        if value is None or len(value) == 0 : 
            result = 'n/a'
        else : 
            result = value.get_text()
        return result
        