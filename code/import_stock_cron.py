"""
Модуль обновление остатков
"""
import b24
import ms
import time
import sys


def main():
    b24_tree = None

    ids = ms.get_products_from_move("enter", 3) | ms.get_products_from_move("loss", 3) | \
          ms.get_products_from_move("retaildemand", 5) | ms.get_products_from_move("retailsalesreturn", 3)

    ids_len = len(ids)
    if ids_len:
        tm = time.strftime('%H:%M', time.localtime())
        print(tm, "Оперативное обновление остатков. Всего товаров - ", ids_len)
    for id in ids:
        b24_id, price, stock = ms.get_product_stock(id)
        # print(b24_id, price, stock)
        if b24_id:
            b24.modify_product_stock(b24_id, price, stock)
        else:
            print("У товара в мс нет b24_id. Добавляю товар.")
            ms_tuple = ms.get_product_info(id)
            if not b24_tree:
                b24_tree = b24.get_tree()
            b24_id = b24.add_product(ms_tuple, b24_tree)
            if b24_id:
                ms.modify_product_b24_id(id, b24_id)
    sys.exit()


if __name__ == "__main__":
    main()