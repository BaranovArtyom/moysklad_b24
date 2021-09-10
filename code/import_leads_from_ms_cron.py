"""
Cron обертка для запсука обновления сдлоек из МС в Б24
"""
import b24
import ms
from settings import STATUS_DICT_MSKEY
import sys


def main():
    b24_ids_set = set()
    orders = ms.get_last_orders()
    for ms_dict in orders:
        if ms_dict['b24_id']:
            lead_id = ms_dict['b24_id']
            b24_dict = b24.get_lead_updates(lead_id)
            if not b24_dict:
                ms.del_order(ms_dict['hash'])
                # print(f"Сделка {lead_id} удалена из Б24")
                continue
            if lead_id in b24_ids_set:
                continue
            b24_products = b24.get_lead_products(lead_id)
            flag = 0
            if ms_dict['comment'] != b24_dict['comment']:
                flag = 1
                # print(ms_dict['comment'])
            if ms_dict['hash'] and ms_dict['hash'] != b24_dict['hash']:
                flag = 1
                # print(ms_dict['hash'])
            if ms_dict['ms_id'] and ms_dict['ms_id'] != b24_dict['ms_id']:
                flag = 1
                # print(ms_dict['ms_id'])
            # state
            ms_state = None
            if ms_dict['state'] in STATUS_DICT_MSKEY:
                ms_state = STATUS_DICT_MSKEY[ms_dict['state']]
            if ms_state and ms_state != b24_dict['state']:
                flag = 1
                # print(ms_state)
            if ms_dict['sklad'] and ms_dict['sklad'] != b24_dict['sklad']:
                flag = 1
                # print(ms_dict['sklad'])
            if flag:
                res = {}
                if ms_state:
                    res['state'] = ms_state
                else:
                    res['state'] = b24_dict['state']
                if ms_dict['date'] > b24_dict['date']:
                    res['comment'] = ms_dict['comment']
                else:
                    res['comment'] = b24_dict['comment']
                res['reserve'] = None
                if ms_dict['products']:
                    res['reserve'] = ms_dict['products'][0]['reserve']
                res['sklad'] = ms_dict['sklad']
                res['ms_id'] = ms_dict['ms_id']
                res['hash'] = ms_dict['hash']
                b24.modify_deal_full(lead_id, res)
                b24_ids_set.add(lead_id)
            # сравниваю товары
            if not ms_dict['products'] and not b24_products:
                continue
            res_flag = 0
            res_new_flag = 0
            for im, ms_product in enumerate(ms_dict['products']):
                flag = 1
                new_flag = 1
                for b24_product in b24_products:
                    if ms_product['href'] == b24_product['href']:
                        ms_dict['products'][im]['id'] = b24_product['id']
                        new_flag = 0
                        if not new_flag and ms_product['price'] == b24_product['price'] and ms_product['quantity'] == b24_product['quantity']:
                            flag = 0
                            break
                if flag:
                    res_flag = 1
                if new_flag and not ms_product['b24_id']:
                    res_new_flag = 1
            if not res_new_flag and not res_flag and len(ms_dict['products']) != len(b24_products):
                res_flag = 1
            # print(res_flag, res_new_flag)
            if res_flag:
                if res_new_flag:
                    print("Добавлен новый товар")
                    b24_products_dict = b24.get_products_names_dict()
                    for im, ms_product in enumerate(ms_dict['products']):
                        if ms_dict['products']['name'] in b24_products_dict:
                            ms_dict['products'][im]['b24_id'] = b24_products_dict[ms_dict['products']['name']]
                        else:
                            print("Товар %s не найдет в Б24" % ms_dict['products']['name'])
                            ms_dict['products'][im]['b24_id'] = None
                # print(lead_id, ms_dict['products'])
                b24.modify_deal_products(lead_id, ms_dict['products'])
    sys.exit()


if __name__ == "__main__":
    main()