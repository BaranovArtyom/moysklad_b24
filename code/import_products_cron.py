"""
Cron обертка для запсука обновления продуктов по списку Б24 - параметры берутся из МС
"""
import b24
import ms
import time
import sys


def main():
    ####### 27.08.2021 делаю полную выгрузку товаров из МС в Б24 ################
    ############################ 20min - 1000 товаров ###########################
    # print("start")
    # product_list = ms.get_products_price_stock(max=None, avito=False)
    # print(len(product_list))
    # # print(product_list)
    # products_names_dict = b24.get_products_names_dict()
    # print(len(products_names_dict))
    # b24_tree = b24.get_tree()
    # # print(b24_tree)
    # for elem in product_list:
    #     if elem[2] in products_names_dict:
    #         # print("mod", products_names_dict[elem[2]], elem)
    #         b24.modify_product_light(products_names_dict[elem[2]], elem)
    #         if not elem[8] and elem[5]:
    #             ms.modify_product_b24_id(elem[5].split("/")[-1], products_names_dict[elem[2]])
    #     else:
    #         # print("add", elem)
    #         b24.add_product(elem, b24_tree)
    ##############################################################################

    ##################### Добавление недостающих товаров #########################
    # product_list = ms.get_products_light()
    # products_names_dict = b24.get_products_names_dict()
    # b24_tree = b24.get_tree()
    # for elem in product_list:
    #     if elem[2] not in products_names_dict:
    #         b24.add_product(elem, b24_tree)
    tm = time.strftime('%H:%M', time.localtime())
    print(f"{tm} Старт получения товаров из Б24")
    b24_products_ids = b24.get_products_ids()
    print("%i товаров получено" % len(b24_products_ids))
    tm = time.strftime('%H:%M', time.localtime())
    print(f"{tm} Старт обновления товаров из Б24")
    for id in b24_products_ids:
        ms_id = b24.get_product_href(id, full=False)
        if not ms_id:
            print("No href for", id)
        else:
            ms_tuple = ms.get_product_info(ms_id)
            b24.modify_product_light(id, ms_tuple)
            if not ms_tuple[8] and ms_tuple[0]:
                ms.modify_product_b24_id(ms_id, id)

    tm = time.strftime('%H:%M', time.localtime())
    print(f"{tm} Обработка завершена")
    sys.exit()


if __name__ == "__main__":
    main()