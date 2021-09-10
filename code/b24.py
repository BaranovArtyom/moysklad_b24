"""
Bitrix 24 API lib. For MoySklad.
"""

import json
from typing import Dict, Set, Union, List

import requests
import time
import datetime
# pip install requests
from settings import B24_CLIENT_ID, B24_CLIENT_SECRET, B24_TOKEN_FILE, B24_TOKEN_EXP_TIME, B24_CATALOG_ID, TRY, \
    SLEEP_TIME, B24_API_LIMIT, B24_FIELD_ART, B24_FIELD_MSLINK, B24_FIELD_STOCK, B24_LEAD_FIELD_MS, \
    B24_LEAD_FIELD_HASH_MS, B24_LAST_LEADS_LIMIT, B24_LEAD_FIELD_SKLAD, B24_LEAD_FIELD_RESERVE, \
    B24_LEAD_FIELD_SKLAD_DICT, B24_FIELD_MSID


def requests_post(url, params=None, json=None):
    """
    Safe variant
    """
    i = 0
    while i < TRY:
        res = requests.post(url, params=params, json=json)
        if res.ok:
            return res
        time.sleep(SLEEP_TIME)
        i += 1
    return res


def requests_get(url, params=None):
    i = 0
    while i < TRY:
        response = requests.get(url, params=params)
        if response.ok:
            return response
        time.sleep(SLEEP_TIME)
        i += 1
    return response


def get_url_token() -> (str, Dict):
    """
    Возвращает URl клиентского api и словарь параметра с токеном
    """
    try:
        with open(B24_TOKEN_FILE) as f:
            tmp = json.load(f)
    except:
        print("Ошибка чтения файла токена b24")
        return None, None
    if tmp['expires'] - round(time.time()) < B24_TOKEN_EXP_TIME:
        url = f"https://oauth.bitrix.info/oauth/token/?grant_type=refresh_token&client_id={B24_CLIENT_ID}&client_secret={B24_CLIENT_SECRET}&refresh_token={tmp['refresh_token']}"
        response = requests.get(url)
        if response.ok:
            print("Произошла перегенарция b24 access_token")
            tmp = json.loads(str(response.text))
            with open(B24_TOKEN_FILE, "w", encoding="utf-8") as f:
                json.dump(tmp, f)
        else:
            print("Ошибка перегенарции b24 access_token")

    return tmp['client_endpoint'], {"auth": tmp['access_token']}


def get_endpoint_fields(endpoint):
    """
    endpoint =
        "crm.deal.fields" - Сделки
        crm.product.fields - Товары
        crm.catalog.list - Список каталогов
        crm.dealcategory.stage.list

    """
    url, params = get_url_token()
    response = requests.get(url + endpoint, params=params)
    res_dict = json.loads(response.text)
    print(res_dict)
    return res_dict


def modify_deal(deal_id: int, ms_id: Union[int, str], ms_hash: str):
    """
    'UF_CRM_1613413069': {'type': 'string', 'isRequired': False, 'isReadOnly': False, 'isImmutable': False, 'isMultiple': False, 'isDynamic': True, 'title': 'UF_CRM_1613413069', 'listLabel': 'Номер заказа МС', 'formLabel': 'Номер заказа МС', 'filterLabel': 'Номер заказа МС'}

    """
    url, params = get_url_token()
    data ={
        "id": deal_id,
        "fields": {
            B24_LEAD_FIELD_MS: ms_id,
            B24_LEAD_FIELD_HASH_MS: ms_hash
        }
    }
    response = requests_post(url + "crm.deal.update", params=params, json=data)
    if response.ok:
        print("ok modify deal", deal_id, ms_id)
        return True
    print("NO modify deal", deal_id, ms_id, response.text)
    return False


def get_tree_ids() -> Dict:
    """
    Выход: id_int: [name_str, parent_id_int] - В Б24 ВСЕ РАВНО В ЧИСЛА КАК СТРОКИ
    """
    res = {}
    url, params = get_url_token()
    start = 0
    id = 0
    id_tmp = 0
    while True:
        try:
            response = requests_get(url + f"crm.productsection.list?start={start}", params=params)
            res_list = json.loads(response.text)['result']
        except:
            return res
        if not res_list:
            break
        for elem in res_list:
            if int(elem['ID']) <= id:
                continue
            res[elem['ID']] = [elem['NAME'], elem['SECTION_ID']]
        id = int(elem['ID'])
        if id == id_tmp:
            break
        id_tmp = id
        start += B24_API_LIMIT
    return res


def get_tree() -> Dict:
    """
    {'ID': '24', 'ORIGINATOR_ID': None, 'ORIGIN_ID': None, 'NAME': 'Товарный каталог CRM', 'XML_ID': 'FUTURE-1C-CATALOG'}
    """
    res = {}
    url, params = get_url_token()
    start = 0
    id = 0
    id_tmp = 0
    while True:
        try:
            response = requests_get(url + f"crm.productsection.list?start={start}", params=params)
            res_list = json.loads(response.text)['result']
        except:
            return res
        if not res_list:
            break
        for elem in res_list:
            if int(elem['ID']) <= id:
                continue
            res[elem['NAME']] = [elem['ID'], elem['SECTION_ID']]
        id = int(elem['ID'])
        if id == id_tmp:
            break
        id_tmp = id
        start += B24_API_LIMIT
    return res


def add_tree_elem(name: str, section_id=None):
    url, params = get_url_token()
    data = {
        "fields": {
            "CATALOG_ID": B24_CATALOG_ID,
            "NAME": name,
            "SECTION_ID": section_id
        }
    }
    response = requests_post(url + "crm.productsection.add", params=params, json=data)
    if response.ok:
        print('ok add dir', name, section_id)
        return True
    print('NO add dir', name, section_id, response.text)
    return False


def modify_tree_elem(name: str, id: str, section_id: str):
    url, params = get_url_token()
    data = {
        "id": id,
        "fields": {
            "CATALOG_ID": B24_CATALOG_ID,
            "NAME": name,
            "SECTION_ID": section_id
        }
    }
    response = requests_post(url + "crm.productsection.update", params=params, json=data)
    if response.ok:
        print('ok modify dir', name, id, section_id)
        return True
    print('NO modify dir', name, id, section_id, response.text)
    return False


def del_tree_elem(id):
    url, params = get_url_token()

    data = {
        "id": id
    }
    response = requests_post(url + "crm.productsection.delete", params=params, json=data)
    if response.ok:
        print("ok del dir", id)
        return True
    print('NO del dir', id, response.text)
    return False


def del_tree():
    url, params = get_url_token()
    start = 0
    id = 0
    id_tmp = 0
    while True:
        try:
            response = requests_get(url + f"crm.productsection.list?start={start}", params=params)
            res_list = json.loads(response.text)['result']
        except:
            break
        if not res_list:
            break
        for elem in res_list:
            del_tree_elem(elem['ID'])
        id = int(elem['ID'])
        if id == id_tmp:
            break
        id_tmp = id
        start += B24_API_LIMIT


def import_tree(ms_id_tree: Dict):
    """
    ms_tree: {"name_str": [id_int, parent_id_int]} parent_id = 0 Корень
    ms_id_tree: {id_int: [name_str, parent_id_int]} parent_id = 0 Корень
    b24_tree: {"name_str": [id_str, section_id_str]} parent_id = None Корень
    Если такое наименовани есть то ОБНОВЛЯЕМ ТОЛЬКО В СЛУЧАЕ ОТСУТВИЯ РОДИТЕЛЯ в Б24 и наличии в МС
    """
    flag = 1
    while flag:
        flag = 0
        for key in ms_id_tree:
            b24_tree = get_tree()
            if ms_id_tree[key][0] not in b24_tree:
                if ms_id_tree[key][1]:
                    if ms_id_tree[ms_id_tree[key][1]][0] in b24_tree:
                        section_id = b24_tree[ms_id_tree[ms_id_tree[key][1]][0]][0]
                        add_tree_elem(ms_id_tree[key][0], section_id)
                    else:
                        flag = 1
                        add_tree_elem(ms_id_tree[key][0])
                else:
                    add_tree_elem(ms_id_tree[key][0])
            elif ms_id_tree[key][1] and not b24_tree[ms_id_tree[key][0]][1]:
                if ms_id_tree[ms_id_tree[key][1]][0] in b24_tree:
                    section_id = b24_tree[ms_id_tree[ms_id_tree[key][1]][0]][0]
                    id = b24_tree[ms_id_tree[key][0]][0]
                    modify_tree_elem(ms_id_tree[key][0], id, section_id)
                else:
                    flag = 1


def import_tree_v2(ms_id_tree: Dict):
    """
    ms_id_tree: {id_int: [name_str, parent_id_int]} parent_id = 0 Корень
    b24_tree: {"name_str": [id_str, section_id_str]} parent_id = None Корень
    Шаги работы:
    1 - первый обхзод по ms_id_tree Если нет такого наименования то он добавляется с Prent_id=None
    2 - второй обход по ms_id_tree Если в ms_id_tree у элемента есть родитель то он модицифируется
    """
    i = 0
    j = 0
    b24_tree = get_tree()
    for key in ms_id_tree:
        if ms_id_tree[key][0] not in b24_tree:
            # print("add", ms_id_tree[key][0])
            if not add_tree_elem(ms_id_tree[key][0]):
                i += 1
        # else:
        #     print("no add")
    time.sleep(SLEEP_TIME * 2)
    b24_tree = get_tree()
    for key in ms_id_tree:
        if ms_id_tree[key][0] in b24_tree and ms_id_tree[key][1] and ms_id_tree[ms_id_tree[key][1]][0] in b24_tree:
            # print("mod", ms_id_tree[key][0], b24_tree[ms_id_tree[key][0]][0], b24_tree[ms_id_tree[ms_id_tree[key][1]][0]][0])
            if not modify_tree_elem(ms_id_tree[key][0], b24_tree[ms_id_tree[key][0]][0], b24_tree[ms_id_tree[ms_id_tree[key][1]][0]][0]):
                j += 1
        # else:
        #     print("no mod")
    print(f"При импорте дерева было {i} ошибок добавления и {j} ошибок изменения родителя.")


def get_group_id_by_name(group_name, b24_tree):
    if group_name:
        if group_name in b24_tree:
            return b24_tree[group_name][0]
    return None


def add_product(ms_tuple, b24_tree):
    """
    ms_tuple: (id: int, articul: str, name: str, description: str, pathName: str, href: str, price: float, stock: Dict[str: int])

    'PRICE', 'CURRENCY_ID'="RUB", 'NAME', 'CODE'('type': 'string'), 'DESCRIPTION',  'SECTION_ID': {'type': 'integer'}
    'PROPERTY_106': {'type': 'product_property', 'propertyType': 'S', 'userType': '', 'title': 'Идентификатор МойСклад'} - в чистой нет
    все, больше полей нет
    """
    url, params = get_url_token()
    section_id = None
    if ms_tuple[4]:
        section_id = get_group_id_by_name(ms_tuple[4].split('/')[-1], b24_tree)
    fields = {
        "NAME": ms_tuple[2],
        "CURRENCY_ID": "RUB",
        "PRICE": ms_tuple[6],
        "DESCRIPTION": ms_tuple[3],
        "SECTION_ID": section_id,
        B24_FIELD_ART: ms_tuple[1],
        B24_FIELD_MSLINK: ms_tuple[5]
    }
    if ms_tuple[7]:
        for key in ms_tuple[7]:
            fields[B24_FIELD_STOCK[key]] = ms_tuple[7][key]
    data = {
        "fields": fields
    }
    response = requests_post(url + "crm.product.add", params=params, json=data)
    if response.ok:
        print("ok add product", ms_tuple)
        return json.loads(response.text)['result']
    print("NO add product", ms_tuple, response.text)
    return None


def modify_product_full(id, ms_tuple, b24_tree):
    """
    ms_tuple: (id: int, articul: str, name: str, description: str, pathName: str, href: str, price: float, stock: Dict[str: int])
    'PRICE', 'CURRENCY_ID'="RUB", 'NAME', 'CODE'('type': 'string'), 'DESCRIPTION',  'SECTION_ID': {'type': 'integer'}
    'PROPERTY_106': {'type': 'product_property', 'propertyType': 'S', 'userType': '', 'title': 'Идентификатор МойСклад'} - в чистой нет
    все, больше полей нет
    """
    url, params = get_url_token()
    section_id = None
    if ms_tuple[4]:
        section_id = get_group_id_by_name(ms_tuple[4].split('/')[-1], b24_tree)
    fields = {
        "CURRENCY_ID": "RUB",
        "PRICE": ms_tuple[6],
        "DESCRIPTION": ms_tuple[3],
        "SECTION_ID": section_id,
        B24_FIELD_ART: ms_tuple[1],
        B24_FIELD_MSLINK: ms_tuple[5]
    }
    if ms_tuple[7]:
        for key in ms_tuple[7]:
            fields[B24_FIELD_STOCK[key]] = ms_tuple[7][key]
    data = {
        "id": id,
        "fields": fields
    }
    response = requests_post(url + "crm.product.update", params=params, json=data)
    if response.ok:
        print("ok full modify product", ms_tuple)
        return True
    print("NO full modify product", ms_tuple, response.text)
    return False


def modify_product_light(id, ms_tuple):
    """
    Modifies only price and stock
    """
    url, params = get_url_token()
    fields = {
        "CURRENCY_ID": "RUB",
        "PRICE": ms_tuple[6],
        "DESCRIPTION": ms_tuple[3],
        B24_FIELD_ART: ms_tuple[1],
        B24_FIELD_MSLINK: ms_tuple[5]
    }
    if ms_tuple[7]:
        for key in ms_tuple[7]:
            fields[B24_FIELD_STOCK[key]] = ms_tuple[7][key]
    data = {
        "id": id,
        "fields": fields
    }
    response = requests_post(url + "crm.product.update", params=params, json=data)
    if response.ok:
        print("ok light modify product", ms_tuple)
        return True
    print("NO light modify product", ms_tuple, response.text)
    return False


def modify_product_stock(id, price, stock):
    """
    Modifies only stock and price
    """
    url, params = get_url_token()
    fields = {
        "CURRENCY_ID": "RUB",
        "PRICE": price,
    }
    for key in stock:
        fields[B24_FIELD_STOCK[key]] = stock[key]
    data = {
        "id": id,
        "fields": fields
    }
    response = requests_post(url + "crm.product.update", params=params, json=data)
    if response.ok:
        print("ok modify stock in b24", id)
        return True
    print("NO modify stock in b24", id, response.text)
    return False


def get_products_ids(limit=None) -> Set:
    """
    Ключи id - int!
    """
    url, params = get_url_token()
    res = set()
    start = 0
    id = 0
    id_tmp = 0
    i = 0
    while True:
        response = requests_get(url + f"crm.product.list?start={start}", params=params)
        try:
            res_list = json.loads(response.text)['result']
        except:
            return res
        if not res_list:
            break
        for elem in res_list:
            elem_id = int(elem['ID'])
            if elem_id <= id:
                continue
            res.add(elem_id)
            i += 1
            if limit and i >= limit:
                return res
        id = elem_id
        if id == id_tmp:
            break
        id_tmp = id
        start += B24_API_LIMIT
        #print(res_list)
    return res


def get_products_names_dict() -> Dict:
    url, params = get_url_token()
    res = dict()
    start = 0
    id = 0
    id_tmp = 0
    while True:
        response = requests_get(url + f"crm.product.list?start={start}", params=params)
        try:
            res_list = json.loads(response.text)['result']
        except:
            return res
        if not res_list:
            break
        for elem in res_list:
            id_int = int(elem['ID'])
            if id_int <= id:
                continue
            res[elem['NAME']] = id_int
        id = id_int
        if id == id_tmp:
            break
        id_tmp = id
        start += B24_API_LIMIT
        #print(res_list)
    return res


def get_products_names() -> Set:
    url, params = get_url_token()
    res = set()
    start = 0
    id = 0
    id_tmp = 0
    while True:
        response = requests_get(url + f"crm.product.list?start={start}", params=params)
        try:
            res_list = json.loads(response.text)['result']
        except:
            return res
        if not res_list:
            break
        for elem in res_list:
            if int(elem['ID']) <= id:
                continue
            res.add(elem['NAME'])
        id = int(elem['ID'])
        if id == id_tmp:
            break
        id_tmp = id
        start += B24_API_LIMIT
    return res


def del_product(id):
    url, params = get_url_token()

    data = {
        "id": id
    }
    response = requests_post(url + "crm.product.delete", params=params, json=data)
    if response.ok:
        print('ok del product', id)
        return True
    print('NO del product', json.loads(response.text))
    return False


def get_product_href(b24_code: Union[int, str], full=True) -> str:
    url, params = get_url_token()
    response = requests_get(url + f"crm.product.get?id={b24_code}", params=params)
    try:
        res_dict = json.loads(response.text)["result"]
    except:
        print(f"Error: getting product {b24_code} href")
        return None
    if res_dict[B24_FIELD_MSLINK]:
        href = res_dict[B24_FIELD_MSLINK]['value']
        if not full:
            href = href.split('/')[-1]
    elif res_dict[B24_FIELD_MSID]:
        href = res_dict[B24_FIELD_MSID]['value']
        if full:
            href = "https://online.moysklad.ru/api/remap/1.2/entity/product/" + href
    else:
        href = None
    return href


def get_lead_products(lead_id) -> List[Dict]:
    url, params = get_url_token()
    response = requests_get(url + f"crm.deal.productrows.get?id={lead_id}", params=params)
    res = []
    try:
        res_list = json.loads(response.text)["result"]
    except:
        print(f"Error: getting lead {lead_id} products")
        return res
    for elem in res_list:
        elem_dict = {}
        elem_dict['id'] = elem['PRODUCT_ID']
        elem_dict['price'] = float(elem['PRICE_ACCOUNT'])
        elem_dict['quantity'] = elem['QUANTITY']
        elem_dict['href'] = get_product_href(elem_dict['id'])
        #print(elem_dict['href'])
        res.append(elem_dict)
    return res


def get_lead_ms_hash(lead_id) -> str:
    url, params = get_url_token()
    response = requests_get(url + f"crm.deal.get?id={lead_id}", params=params)
    # print(response.text)
    try:
        res_dict = json.loads(response.text)["result"]
    except:
        print(f"Error: getting lead {lead_id} ms hash")
        return None
    return res_dict[B24_LEAD_FIELD_HASH_MS]


def get_last_leads(limit=B24_LAST_LEADS_LIMIT) -> List:
    url, params = get_url_token()
    response = requests.get(url + f"crm.deal.list?order[DATE_MODIFY]=DESC", params=params)
    res = []
    try:
        res_list = json.loads(response.text)["result"]
    except:
        print(f"Error: getting leads list")
        return res
    i = 0
    # print(res_list)
    for elem in res_list:
        res.append(elem['ID'])
        i += 1
        if i >= limit:
            break
    return res



def get_lead_updates(lead_id) -> Dict:
    """
    updates_dict['hash'], updates_dict['comment'], updates_dict['sklad'], updates_dict['reserve']
    все строки! reserve = '1' or '0'
    """
    url, params = get_url_token()
    updates_dict = dict()
    response = requests_get(url + f"crm.deal.get?id={lead_id}", params=params)
    try:
        res_dict = json.loads(response.text)["result"]
    except:
        print(f"Error: getting lead {lead_id} update")
        return None
    # print(res_dict)
    updates_dict['state'] = res_dict['STAGE_ID']
    updates_dict['hash'] = res_dict[B24_LEAD_FIELD_HASH_MS]
    updates_dict['ms_id'] = res_dict[B24_LEAD_FIELD_MS]
    updates_dict['comment'] = res_dict['COMMENTS']
    updates_dict['sklad'] = B24_LEAD_FIELD_SKLAD_DICT[res_dict[B24_LEAD_FIELD_SKLAD]]
    updates_dict['reserve'] = res_dict[B24_LEAD_FIELD_RESERVE]
    updates_dict['date'] = datetime.datetime.strptime(res_dict['DATE_MODIFY'], "%Y-%m-%dT%H:%M:%S%z").timestamp()
    # print(updates_dict['date'])
    return updates_dict


def modify_deal_full(deal_id: int, res: Dict):
    sklad_id = ''
    if res['sklad']:
        for key in B24_LEAD_FIELD_SKLAD_DICT:
            if B24_LEAD_FIELD_SKLAD_DICT[key] == res['sklad']:
                sklad_id = key
                break
    reserve = '0'
    if res['reserve']:
        reserve = '1'

    url, params = get_url_token()
    data ={
        "id": deal_id,
        "fields": {
            'STAGE_ID': res['state'],
            'COMMENTS': res['comment'],
            B24_LEAD_FIELD_RESERVE: reserve,
            B24_LEAD_FIELD_SKLAD: sklad_id,
            B24_LEAD_FIELD_MS: res['ms_id'],
            B24_LEAD_FIELD_HASH_MS: res['hash']
        }
    }
    response = requests_post(url + "crm.deal.update", params=params, json=data)
    if response.ok:
        print("b24 ok full modify deal", deal_id)
        return True
    print("b24 NO full modify deal", deal_id, response.text)
    return False
    # print(data)


def modify_deal_products(deal_id: int, products: List[Dict]):
    """
    На вход [{'id': 1, 'price': 400.0, 'quantity': 1, 'reserve': 0, 'href': 'https://online.moysklad.ru/api/remap/1.2/entity/product/8bd8b9ea-f6ca-11eb-0a80-03b500114ae2', 'name': 'Сувенир гитара брелок'}]
    """
    url, params = get_url_token()
    rows = []
    for product in products:
        if product['b24_id']:
            rows.append({ "PRODUCT_ID": product['b24_id'], "PRICE": product['price'], "QUANTITY": product['quantity'] })
    data ={
        "id": deal_id,
        "rows": rows
    }
    response = requests_post(url + "crm.deal.productrows.set", params=params, json=data)
    if response.ok:
        print("b24 ok modify deal prodcut", deal_id)
        return True
    print("b24 NO modify deal product", deal_id, response.text)
    return False
    # print(data)


def del_lead(lead_id):
    url, params = get_url_token()
    data = {
        "id": lead_id
    }
    response = requests_post(url + "crm.deal.delete", params=params, json=data)
    if response.ok:
        print("ok b24 del deal", lead_id)
        return True
    print("NO b24 del deal", lead_id, response.text)
    return False