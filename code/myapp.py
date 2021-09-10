"""
Веб хук для B24-МС интеграции

Новая сделка
http://a0567593.xsph.ru/site.wsgi/?type=new&id={{ID}}&comment={{Комментарий}}&name={{Контакт > printable}}&tel={{Контакт: Телефон (текст)}}&reserve={{Резервировать товар?}}&state={{Стадия сделки (текст)}}&sklad={{Склад резервирования (текст)}}
reserve={{Резервировать товар?}} 'N' или 'Y'

Смена статуса
http://a0567593.xsph.ru/site.wsgi/?type=state&id={{ID}}&state={{Стадия сделки (текст)}}

Вбехку в интеграциях на Обновление сделки
http://a0567593.xsph.ru/site.wsgi/?type=update

Тесты
http://127.0.0.1:5000/?type=new&id=26&comment=%D0%9A%D0%BE%D0%BC%D0%BC%D0%B5%D0%BD%D1%82%D0%B0%D1%80%D0%B8%D0%B9&name=oleg&tel=89179021656&reserve=N&product=342&sum=1000
http://a0567593.xsph.ru/site.wsgi/?type=new&id=34&comment=svfd&name=oleg&tel=89179021656&reserve=Y&sum=600

"""


from flask import Flask, request
from settings import DIR_SERVER, FILE_TEST, LOCAL_MODE, STATUS_DICT_B24KEY
import ms
import b24


app = Flask(__name__)


@app.route("/", methods=['GET', 'POST'])
def hello():
    type = request.args.get('type', default=None)
    id = request.args.get('id', default=None)
    comment = request.args.get('comment', default=None)
    name = request.args.get('name', default=None)
    tel = request.args.get('tel', default=None)
    reserve = request.args.get('reserve', default=None)
    state = request.args.get('state', default=None)
    sklad = request.args.get('sklad', default=None)
    products = None

    if type == "new" and id and name:
        flag = 1
        ms_lat_leads = ms.get_last_orders()
        int_id = int(id)
        for lead in ms_lat_leads:
            if int_id == lead['b24_id']:
                flag = 0
                products = b24.get_lead_products(id)
                updates_dict = b24.get_lead_updates(id)
                if not updates_dict['hash']:
                    continue
                if products:
                    products_meta = ms.make_products_meta(products, updates_dict['reserve'])
                else:
                    products_meta = None
                ms.modify_order_updates(updates_dict, products_meta)
                break
        if flag:
            contragent_meta = ms.add_contragent(name, tel)
            products = b24.get_lead_products(id)
            if products:
                products_meta = ms.make_products_meta(products, reserve)
            else:
                products_meta = None
            order_id, order_hash_id = ms.post_order(contragent_meta, products_meta, id, comment, sklad)
            if order_id:
                b24.modify_deal(id, order_id, order_hash_id)
    elif type == "state" and id and state:
        hash_id = b24.get_lead_ms_hash(id)
        if hash_id and (state in STATUS_DICT_B24KEY):
            ms.modify_order_state(hash_id, STATUS_DICT_B24KEY[state])
    elif type == "update": #TODO переделать на получение модифицированных сделок из б24 сперва
        leads = b24.get_last_leads()
        for lead_id in leads:
            products = b24.get_lead_products(lead_id)
            updates_dict = b24.get_lead_updates(lead_id)
            if not updates_dict['hash']:
                continue
            if products:
                products_meta = ms.make_products_meta(products, updates_dict['reserve'])
            else:
                products_meta = None
            ms.modify_order_updates(updates_dict, products_meta)


    test_file = DIR_SERVER + FILE_TEST
    if LOCAL_MODE:
        test_file = FILE_TEST
    with open(test_file, 'a') as f:
        f.write(f"{type}\n{id}\n{comment}\n{name}\n{tel}\n{products}\n{reserve}\n{state}\n{sklad}\n\n")

    return 'ok', 200


# Для загрузки xml файлов обяьлений на Хостинг
@app.route("/upload", methods=['POST'])
def upload():
    file = request.files.get('file')
    file.save(DIR_SERVER + file.filename)
    return 'ok', 200


if __name__ == "__main__":
    app.run()