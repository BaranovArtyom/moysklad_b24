"""
MoySklad lib for B24-MS integration
"""


from typing import Dict, Union, List, Tuple, Set
import requests
import time
import datetime
import json
from settings import TRY, SLEEP_TIME, MS_AUTH, MS_HEADERS, MS_PRICE_NAME, MS_ORG_META, MS_FIELD_B24LEADID_INORDER, \
    MS_LAST_LEADS_LIMIT, MS_FILED_PRODUCT_B24_ID, MS_LAST_TIME_MOVES, MS_LAST_TIME_LIMIT


def requests_get(url):
    i = 0
    resp = None
    while i < TRY:
        try:
            resp = requests.get(url, auth=MS_AUTH, headers=MS_HEADERS)
        except:
            time.sleep(SLEEP_TIME)
            i += 1
        else:
            break
    return resp


def add_contragent(name, phone=None):
    url = "https://online.moysklad.ru/api/remap/1.2/entity/counterparty"

    data = {
        "name": name,
        "phone": phone
    }
    resp = requests.post(url, auth=MS_AUTH, headers=MS_HEADERS, json=data)
    res_dict = resp.json()
    if resp.ok:
        print("MS ok add contragent", name, phone)
        return res_dict["meta"]
    print(res_dict)
    return None


def make_sklad_meta(sklad):
    """
        Иваново Склад
        'store': {'meta': {'href': 'https://online.moysklad.ru/api/remap/1.2/entity/store/e3c1f8e2-46c9-11eb-0a80-03e500486a28', 'metadataHref': 'https://online.moysklad.ru/api/remap/1.2/entity/store/metadata', 'type': 'store', 'mediaType': 'application/json', 'uuidHref': 'https://online.moysklad.ru/app/#warehouse/edit?id=e3c1f8e2-46c9-11eb-0a80-03e500486a28'}}

        Москва Можайская
        'store': {'meta': {'href': 'https://online.moysklad.ru/api/remap/1.2/entity/store/a508c5a7-4371-11eb-0a80-0682002b5266', 'metadataHref': 'https://online.moysklad.ru/api/remap/1.2/entity/store/metadata', 'type': 'store', 'mediaType': 'application/json', 'uuidHref': 'https://online.moysklad.ru/app/#warehouse/edit?id=a508c5a7-4371-11eb-0a80-0682002b5266'}}

        СКЛАД Сухаревская МСК
        'store': {'meta': {'href': 'https://online.moysklad.ru/api/remap/1.2/entity/store/c3714385-4521-11eb-0a80-07c50022cccc', 'metadataHref': 'https://online.moysklad.ru/api/remap/1.2/entity/store/metadata', 'type': 'store', 'mediaType': 'application/json', 'uuidHref': 'https://online.moysklad.ru/app/#warehouse/edit?id=c3714385-4521-11eb-0a80-07c50022cccc'}}

        СПб Звенигородская
        'store': {'meta': {'href': 'https://online.moysklad.ru/api/remap/1.2/entity/store/7f8b1646-03f5-11eb-0a80-098b000df149', 'metadataHref': 'https://online.moysklad.ru/api/remap/1.2/entity/store/metadata', 'type': 'store', 'mediaType': 'application/json', 'uuidHref': 'https://online.moysklad.ru/app/#warehouse/edit?id=7f8b1646-03f5-11eb-0a80-098b000df149'}}
        """
    store = None
    if sklad.find("Иваново Склад") != -1:
        store = {'meta': {'href': 'https://online.moysklad.ru/api/remap/1.2/entity/store/e3c1f8e2-46c9-11eb-0a80-03e500486a28', 'metadataHref': 'https://online.moysklad.ru/api/remap/1.2/entity/store/metadata', 'type': 'store', 'mediaType': 'application/json', 'uuidHref': 'https://online.moysklad.ru/app/#warehouse/edit?id=e3c1f8e2-46c9-11eb-0a80-03e500486a28'}}
    elif sklad.find("Москва Можайская") != -1:
        store = {'meta': {'href': 'https://online.moysklad.ru/api/remap/1.2/entity/store/a508c5a7-4371-11eb-0a80-0682002b5266', 'metadataHref': 'https://online.moysklad.ru/api/remap/1.2/entity/store/metadata', 'type': 'store', 'mediaType': 'application/json', 'uuidHref': 'https://online.moysklad.ru/app/#warehouse/edit?id=a508c5a7-4371-11eb-0a80-0682002b5266'}}
    elif sklad.find("СКЛАД Сухаревская МСК") != -1:
        store = {'meta': {'href': 'https://online.moysklad.ru/api/remap/1.2/entity/store/c3714385-4521-11eb-0a80-07c50022cccc', 'metadataHref': 'https://online.moysklad.ru/api/remap/1.2/entity/store/metadata', 'type': 'store', 'mediaType': 'application/json', 'uuidHref': 'https://online.moysklad.ru/app/#warehouse/edit?id=c3714385-4521-11eb-0a80-07c50022cccc'}}
    elif sklad.find("СПб Звенигородская") != -1:
        store = {'meta': {'href': 'https://online.moysklad.ru/api/remap/1.2/entity/store/7f8b1646-03f5-11eb-0a80-098b000df149', 'metadataHref': 'https://online.moysklad.ru/api/remap/1.2/entity/store/metadata', 'type': 'store', 'mediaType': 'application/json', 'uuidHref': 'https://online.moysklad.ru/app/#warehouse/edit?id=7f8b1646-03f5-11eb-0a80-098b000df149'}}
    return store


def make_products_meta(products: List[Dict], reserve):
    """
    "positions": [{
                "quantity": 10,
                "price": 100,
                "discount": 0,
                "vat": 0,
                "assortment": {
                  "meta": {
                    "href": "https://online.moysklad.ru/api/remap/1.2/entity/product/8b382799-f7d2-11e5-8a84-bae5000003a5",
                    "type": "product",
                    "mediaType": "application/json"
                  }
                },
                "reserve": 10
              }]
    """
    for i in range(len(products)):
        mshref = products[i]['href']
        price = products[i]['price'] * 100
        quantity = products[i]['quantity']
        if reserve == 'Y' or reserve == '1':
            reserve_num = quantity
        else:
            reserve_num = 0
        products[i] = {
                "quantity": quantity,
                "price": price,
                "discount": 0,
                "vat": 0,
                "assortment": {
                  "meta": {
                    "href": mshref,
                    "type": "product",
                    "mediaType": "application/json"
                  }
                },
                "reserve": reserve_num
        }
    return products


def post_order(contragent_meta, products_meta, id, comment, sklad) -> (str, str):
    """
    ПРОВЕРИТЬ ID! Вызывающий не проверяет.
    #     contragent_meta = {
    #     "href": "https://online.moysklad.ru/api/remap/1.2/entity/counterparty/1888be2d-84d6-11eb-0a80-0581001bad77",
    #     "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/counterparty/metadata",
    #     "type": "counterparty",
    #     "mediaType": "application/json",
    #     "uuidHref": "https://online.moysklad.ru/app/#company/edit?id=1888be2d-84d6-11eb-0a80-0581001bad77"
    # }
    """
    try:
        id = int(id)
    except:
        id = 0

    store = make_sklad_meta(sklad)

    url = "https://online.moysklad.ru/api/remap/1.2/entity/customerorder"

    data = {
        "name": "",
        "description": comment,
        "positions": products_meta,
        "organization": {"meta": MS_ORG_META},
        "agent": {"meta": contragent_meta},
        'store': store,
        "attributes": [{
            "meta": {
                "href": f"https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/attributes/{MS_FIELD_B24LEADID_INORDER}",
                "type": "attributemetadata",
                "mediaType": "application/json"
            },
            "id": MS_FIELD_B24LEADID_INORDER,
            "value": id
        }],
    }

    res = {}
    resp = requests.post(url, auth=MS_AUTH, headers=MS_HEADERS, json=data)
    res_dict = json.loads(resp.text)
    if resp.ok:
        print("MS ok add lead", res_dict['name'])
        return res_dict['name'], res_dict['id']
    print("MS NO add lead", res_dict)
    return None, None


def get_state_meta(ms_state_name: str) -> Dict:
    res = dict()

    if ms_state_name == "Новая":
        res["meta"] = {
        "href" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/f6b26c7c-7e80-11eb-0a80-08300019e9a5",
        "metadataHref" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata",
        "type" : "state",
        "mediaType" : "application/json"
        }
    elif ms_state_name == "Приедут в магазин":
        res["meta"] = {
        "href" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/d84478cd-33ea-11eb-0a80-048e000f90f5",
        "metadataHref" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata",
        "type" : "state",
        "mediaType" : "application/json"
        }
    elif ms_state_name == "Доставки":
        res["meta"] = {
        "href" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/072552d0-33ed-11eb-0a80-015d00100c96",
        "metadataHref" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata",
        "type" : "state",
        "mediaType" : "application/json"
        }
    elif ms_state_name == "Отправка СДЭК":
        res["meta"] = {
        "href" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/51f4b6f3-3fa3-11eb-0a80-028d0021741f",
        "metadataHref" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata",
        "type" : "state",
        "mediaType" : "application/json"
        }
    elif ms_state_name == "ЗАКАЗЫ В РАБОТЕ":
        res["meta"] = {
        "href" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/3b40e39f-33f0-11eb-0a80-00ad0010a63c",
        "metadataHref" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata",
        "type" : "state",
        "mediaType" : "application/json"
        }
    elif ms_state_name == "Отложка":
        res["meta"] = {
        "href" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/b69abc45-42a2-11eb-0a80-0467001e57e7",
        "metadataHref" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata",
        "type" : "state",
        "mediaType" : "application/json"
        }
    elif ms_state_name == "Завершен ПОЛОЖИТЕЛЬНО":
        res["meta"] = {
        "href" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/3fe8db2e-33eb-11eb-0a80-03a6000f6369",
        "metadataHref" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata",
        "type" : "state",
        "mediaType" : "application/json"
        }
    elif ms_state_name == "Приехали, пока думают":
        res["meta"] = {
        "href" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/2a20bace-503f-11eb-0a80-05ec0020e130",
        "metadataHref" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata",
        "type" : "state",
        "mediaType" : "application/json"
        }
    elif ms_state_name == "Уточнение наличия товаров":
        res["meta"] = {
        "href" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/99d88b8d-7c24-11eb-0a80-0188000f53c1",
        "metadataHref" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata",
        "type" : "state",
        "mediaType" : "application/json"
        },
    elif ms_state_name == "Завершен  ОТРИЦАТЕЛЬНО":
        res["meta"] = {
        "href" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/bd0dc674-9789-11eb-0a80-0d1a001b7f57",
        "metadataHref" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata",
        "type" : "state",
        "mediaType" : "application/json"
        },
    elif ms_state_name == "Ждут привоза":
        res["meta"] = {
        "href" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/55de5082-9ad4-11eb-0a80-0028001e3884",
        "metadataHref" : "https://online.moysklad.ru/api/remap/1.2/entity/customerorder/metadata",
        "type" : "state",
        "mediaType" : "application/json"
        }
    else:
        print("NO modify status: Статус не найден")

    return res


def modify_order_state(hash_id, b24_state):
    state_meta = get_state_meta(b24_state)
    url = f"https://online.moysklad.ru/api/remap/1.2/entity/customerorder/{hash_id}"
    #print(state_meta)
    data = {
        "state": state_meta,
    }

    resp = requests.put(url, auth=MS_AUTH, headers=MS_HEADERS, json=data)
    #res_dict = json.loads(resp.text)
    if resp.ok:
        print("MS ok modify lead state")
        return True
    print("MS NO modify lead state", resp.text)
    return False


def get_entity_meta(entity="organization"):
    """
    Получения meta словарей для сщуностей для последующего их использования в запросах
    entity =
        organization - Наши организации
        counterparty - Контрагенты
        customerorder - Заказы
    """
    url = f"https://online.moysklad.ru/api/remap/1.2/entity/{entity}"
    resp = requests_get(url)
    print(resp.text)


def get_ms_groups(tree=False) -> Union[List, Dict]:
    """
    Получение списка групп Моего склада
    """
    groups = []
    groups_dict = {}
    request_url = "https://online.moysklad.ru/api/remap/1.2/entity/productfolder"
    resp = requests_get(request_url)
    rows = resp.json()['rows']
    if not tree:
        for row in rows:
            if not len(row['pathName']):
                groups.append(row['name'])
        return groups
    else:
        i = 1
        for row in rows:
            if row['name'] not in groups_dict:
                groups_dict[row['name']] = [i, 0]
                i += 1
            parent = row['pathName'].split('/')[-1]
            if len(parent):
                if parent not in groups_dict:
                    groups_dict[parent] = [i, 0]
                    i += 1
                groups_dict[row['name']][1] = groups_dict[parent][0]
        return groups_dict


def convert_groups_dict(name_dict):
    """
    Мз словаря с ключами Имя группы, делает словарь с ключом id  {"id_int": [name_str, section_id_int]}
    """
    id_dict = {}
    for key in name_dict:
        id_dict[name_dict[key][0]] = [key, name_dict[key][1]]
    return id_dict


def get_products_price_stock(max=None, avito=False) -> List[Tuple]:
    """
    Получение всех товаров и цен
    "https://online.moysklad.ru/api/remap/1.2/entity/product" "rows" - список элементов, в нем
    "code", "id", "name", "pathName"='Ударные/Барабанные палочки, аксессуары/Барабанные палочки' или просто ''
    "meta""uuidHref" - ссылка, "salePrices"-список словарей цен, 'description'
    product = ['meta']['href'] - для фильтрации при полоучении остатка
    f"https://online.moysklad.ru/api/remap/1.2/report/stock/bystore?filter=product={product}"
    stock_list = res_dict['rows'][0]['stockByStore']
    Возвращаю
    res = [(id: int, articul: str, name: str, description: str, pathName: str, href: str, price: float, stock: Dict[str: int]), b24_id: int]
    """
    limit = 1000
    offset = 0
    size = 1001
    res = []
    i = 0
    while limit + offset < size:
        request_url = f"https://online.moysklad.ru/api/remap/1.2/entity/product?limit={limit}&offset={offset}"
        if avito:
            request_url = f"https://online.moysklad.ru/api/remap/1.2/entity/product?filter=https://online.moysklad.ru/api/remap/1.2/entity/product/metadata/attributes/b09eecbb-64b3-11eb-0a80-06b400268338=true&limit={limit}&offset={offset}"
        resp = requests_get(request_url)
        res_dict = json.loads(str(resp.text))
        size = res_dict['meta']['size']
        # print(size)
        for elem in res_dict['rows']:
            # print(elem)
            # Получаю остаток
            product = elem['meta']['href']
            url2 = f"https://online.moysklad.ru/api/remap/1.2/report/stock/bystore?filter=product={product}"
            resp2 = requests_get(url2)
            res_dict2 = json.loads(str(resp2.text))
            stock = {}
            if res_dict2['rows']:
                for el in res_dict2['rows'][0]['stockByStore']:
                    #stock[el['name']] = int(el['stock'] - el['reserve'])
                    stock[el['name']] = str(int(el['stock'])) + "(" + str(int(el['stock'] - el['reserve'])) + ")"
            # Price
            price = elem['salePrices'][0]['value'] / 100
            for el in elem['salePrices']:
                if el['priceType']['name'] == MS_PRICE_NAME:
                    price = el['value'] / 100

            b24_id = None
            if 'attributes' in elem:
                for atr in elem['attributes']:
                    if atr['id'] == MS_FILED_PRODUCT_B24_ID:
                        b24_id = atr['value']
                        break
            # href = elem["meta"]["uuidHref"]
            href = product
            if 'description' in elem:
                description = elem['description']
            else:
                description = ""
            if 'article' in elem:
                article = elem['article']
            else:
                article = ""
            res.append((elem['id'], article, elem['name'], description, elem['pathName'], href, price, stock, b24_id))
            #print((elem['id'], article, elem['name'], description, elem['pathName'], href, price, stock)) #FIXME del
            i += 1
            if max and i >= max:
                return res

        offset += 1000
    return res


def get_products_light() -> List[Tuple]:
    """
    Без остатко по складам!
    Без B24_id!
    res = [(id: int, articul: str, name: str, description: str, pathName: str, href: str, price: float, None)]
    последнее значение для совместимости с ms_tuple
    """
    limit = 1000
    offset = 0
    size = 1001
    res = []
    while limit + offset < size:
        request_url = f"https://online.moysklad.ru/api/remap/1.2/entity/product?limit={limit}&offset={offset}"
        resp = requests_get(request_url)
        res_dict = json.loads(str(resp.text))
        size = res_dict['meta']['size']
        for elem in res_dict['rows']:
            # Получаю остаток
            href = elem['meta']['href']
            # Price
            price = elem['salePrices'][0]['value'] / 100
            for el in elem['salePrices']:
                if el['priceType']['name'] == MS_PRICE_NAME:
                    price = el['value'] / 100
            if 'description' in elem:
                description = elem['description']
            else:
                description = ""
            if 'article' in elem:
                article = elem['article']
            else:
                article = ""
            res.append((elem['id'], article, elem['name'], description, elem['pathName'], href, price, None))
            #print((elem['id'], article, elem['name'], description, elem['pathName'], href, price, stock)) #FIXME del

        offset += 1000
    return res


def get_product_info(ms_id: str) -> Tuple:
    """
    (id: int, articul: str, name: str, description: str, pathName: str, href: str, price: float, stock: Dict[str: int], b24_id: int)
    ver2 - Остаток передается не как число а как строка 1(2)
    """
    url = f"https://online.moysklad.ru/api/remap/1.2/entity/product/{ms_id}"
    resp = requests_get(url)
    result = (None, None, None, None, None, None, None, None, None)
    if not resp.ok:
        return result
    try:
        res_dict = json.loads(str(resp.text))
    except:
        return result
    if 'name' not in res_dict:
        return result
    # print(res_dict)
    # Price
    if 'salePrices' in res_dict and res_dict['salePrices']:
        price = res_dict['salePrices'][0]['value'] / 100
        for el in res_dict['salePrices']:
            if el['priceType']['name'] == MS_PRICE_NAME:
                price = el['value'] / 100
    else:
        price = None

    b24_id = None
    if 'attributes' in res_dict:
        for atr in res_dict['attributes']:
            if atr['id'] == MS_FILED_PRODUCT_B24_ID:
                b24_id = atr['value']
                break

    if 'description' in res_dict:
        description = res_dict['description']
    else:
        description = ""
    if 'article' in res_dict:
        article = res_dict['article']
    else:
        article = ""

    ms_href = "https://online.moysklad.ru/api/remap/1.2/entity/product/" + ms_id
    url2 = f"https://online.moysklad.ru/api/remap/1.2/report/stock/bystore?filter=product={ms_href}"
    resp2 = requests_get(url2)
    res_dict2 = json.loads(str(resp2.text))
    stock = {}
    if res_dict2['rows']:
        for el in res_dict2['rows'][0]['stockByStore']:
            #stock[el['name']] = int(el['stock'] - el['reserve'])
            stock[el['name']] = str(int(el['stock'])) + "(" +  str(int(el['stock'] - el['reserve'])) + ")"

    return (ms_id, article, res_dict['name'], description, res_dict['pathName'], ms_href, price, stock, b24_id)


def get_ms_order(id):
    request_url = f"https://online.moysklad.ru/api/remap/1.2/entity/customerorder/{id}"
    resp = requests_get(request_url)
    print(resp.text)


def modify_order_updates(updates_dict, products_meta):
    """
    Вход: updates_dict['hash'], updates_dict['comment'], updates_dict['sklad'], updates_dict['reserve']
    reserve = 1 or 0, остальные строки
    """
    url = f"https://online.moysklad.ru/api/remap/1.2/entity/customerorder/{updates_dict['hash']}"
    store = make_sklad_meta(updates_dict['sklad'])
    data = {
        "description": updates_dict['comment'],
        "positions": products_meta
    }
    if store:
        data['store'] = store
    resp = requests.put(url, auth=MS_AUTH, headers=MS_HEADERS, json=data)
    if resp.ok:
        print("MS ok modify lead")
        return True
    print("MS NO modify lead", resp.text)
    return False


def get_last_orders(limit=MS_LAST_LEADS_LIMIT) -> List:
    # request_url = f"https://online.moysklad.ru/api/remap/1.2/entity/customerorder?order=updated,desc&limit={limit}"
    from_time = datetime.datetime.fromtimestamp(int(time.time()) - MS_LAST_TIME_LIMIT).strftime('%Y-%m-%d %H:%M')
    request_url = f"https://online.moysklad.ru/api/remap/1.2/entity/customerorder?order=updated,desc&filter=updated>{from_time}"
    resp = requests_get(request_url)
    # print(resp.text)
    res = []
    try:
        res_list = resp.json()['rows']
    except:
        return res
    for elem in res_list:
        # print(elem)
        dic = {}
        dic['date'] = datetime.datetime.strptime(elem['updated'], "%Y-%m-%d %H:%M:%S.%f").timestamp()
        # print(dic['date'])
        dic['b24_id'] = None
        if 'attributes' in elem:
            attributes = elem['attributes']
            for el in attributes:
                if el['id'] == MS_FIELD_B24LEADID_INORDER:
                    dic['b24_id'] = el['value']
                    break
        dic['comment'] = ''
        if 'description' in elem:
            dic['comment'] = elem['description']
        dic['hash'] = elem['id']
        dic['ms_id'] = elem['name']
        dic['sklad'] = None
        if 'store' in elem and elem['store']:
            store_href = elem['store']['meta']['href']
            resp2 = requests_get(store_href)
            dic['sklad'] = resp2.json()['name']
        dic['state'] = ''
        if 'state' in elem and elem['state']:
            state_href = elem['state']['meta']['href']
            resp2 = requests_get(state_href)
            dic['state'] = resp2.json()['name']
        dic['products'] = []
        if 'positions' in elem and elem['positions']:
            # print(elem['positions'])
            positions_href = elem['positions']['meta']['href']
            resp2 = requests_get(positions_href)
            products_list = resp2.json()['rows']
            # print(products_list)
            for product_dict in products_list:
                el_dict = {}
                el_dict['price'] = product_dict['price'] / 100
                el_dict['quantity'] = int(product_dict['quantity'])
                el_dict['reserve'] = int(product_dict['reserve'])
                el_dict['href'] = product_dict['assortment']['meta']['href']
                #el_dict['id'] = product_dict['id']
                resp3 = requests_get(el_dict['href'])
                res_dict3 = resp3.json()
                el_dict['b24_id'] = None
                if 'attributes' in res_dict3:
                    for atr in res_dict3['attributes']:
                        if atr['id'] == MS_FILED_PRODUCT_B24_ID:
                            el_dict['b24_id'] = atr['value']
                            break
                el_dict['name'] = res_dict3['name']
                dic['products'].append(el_dict)

        res.append(dic)
    return res


def del_order(ms_hash):
    url = f"https://online.moysklad.ru/api/remap/1.2/entity/customerorder/{ms_hash}"
    resp = requests.delete(url, auth=MS_AUTH, headers=MS_HEADERS)
    if resp.ok:
        print("MS ok del lead", ms_hash)
        return True
    print("MS NO del lead", resp.text)
    return False


def check_ms_order(id):
    request_url = f"https://online.moysklad.ru/api/remap/1.2/entity/customerorder/{id}"
    resp = requests_get(request_url)
    if resp.ok:
        try:
            res = resp.json()
        except:
            print("Check ms order: json error", id)
            return False
        if 'deleted' in res:
            print("Check ms order: 'deleted' in ms.lead", id)
            return False
    else:
        print("Check ms order: resp error", id)
        return False
    return True


def modify_product_b24_id(ms_id: str, b24_id: int):
    """
    ms_id - hash
    """
    url = f"https://online.moysklad.ru/api/remap/1.2/entity/product/{ms_id}"
    data = {
        "attributes": [{
            "meta": {
                "href": f"https://online.moysklad.ru/api/remap/1.2/entity/product/metadata/attributes/{MS_FILED_PRODUCT_B24_ID}",
                "type": "attributemetadata",
                "mediaType": "application/json"
            },
            "id": MS_FILED_PRODUCT_B24_ID,
            "value": b24_id
        }],
    }
    resp = requests.put(url, auth=MS_AUTH, headers=MS_HEADERS, json=data)
    if resp.ok:
        print("MS ok modify product b24_id")
        return True
    print("MS NO modify product b24_id", resp.text)
    return False


def get_products_from_move(type='enter', limit=10) -> Set:
    """
    type = enter, loss, retaildemand, retailsalesreturn
    """
    # Оприходывания
    #url = f"https://online.moysklad.ru/api/remap/1.2/entity/{type}?limit={limit}&order=created,desc"
    from_time = datetime.datetime.fromtimestamp(int(time.time()) - MS_LAST_TIME_MOVES).strftime('%Y-%m-%d %H:%M')
    url = f"https://online.moysklad.ru/api/remap/1.2/entity/{type}?order=updated,desc&filter=updated>{from_time}"
    resp = requests_get(url)
    res = set()
    if not resp.ok:
        return res
    try:
        res_list = json.loads(str(resp.text))['rows']
    except:
        return res
    # print(resp.text)
    for elem in res_list:
        # print(elem['created'])
        if 'positions' in elem:
            url2 = elem['positions']['meta']['href']
            resp2 = requests_get(url2)
            if not resp2.ok:
                continue
            try:
                res_list2 = json.loads(str(resp2.text))['rows']
            except:
                continue
            for el in res_list2:
                if 'assortment' in el:
                    id = el['assortment']['meta']['href'].split('/')[-1]
                    res.add(id)

    return res


def get_product_stock(ms_id: str) -> Tuple:
    """
    Для оперативного обновления остатокв, возвращает b24_id: int, price: int, stock_dict: Dict
    """

    url = f"https://online.moysklad.ru/api/remap/1.2/entity/product/{ms_id}"
    resp = requests_get(url)
    result = None, None, None
    if not resp.ok:
        return result
    try:
        res_dict = json.loads(str(resp.text))
    except:
        return result
    b24_id = None
    if 'attributes' in res_dict:
        for atr in res_dict['attributes']:
            if atr['id'] == MS_FILED_PRODUCT_B24_ID:
                b24_id = atr['value']

    if 'salePrices' in res_dict and res_dict['salePrices']:
        price = res_dict['salePrices'][0]['value'] / 100
        for el in res_dict['salePrices']:
            if el['priceType']['name'] == MS_PRICE_NAME:
                price = el['value'] / 100
    else:
        price = None

    ms_href = "https://online.moysklad.ru/api/remap/1.2/entity/product/" + ms_id
    url2 = f"https://online.moysklad.ru/api/remap/1.2/report/stock/bystore?filter=product={ms_href}"
    resp2 = requests_get(url2)
    res_dict2 = json.loads(str(resp2.text))
    stock = {}
    if res_dict2['rows']:
        for el in res_dict2['rows'][0]['stockByStore']:
            #stock[el['name']] = int(el['stock'] - el['reserve'])
            stock[el['name']] = str(int(el['stock'])) + "(" +  str(int(el['stock'] - el['reserve'])) + ")"

    return b24_id, price, stock