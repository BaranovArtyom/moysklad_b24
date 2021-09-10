"""
Модуль для запуска по расписанию
Удаляет сделки из МС после удаления их в Б24
Проходит по списку Сделок из Б24 - берет хэш МС Заказа в МС -
и если такого Заказа нет в МС то удаляем его в Б24
"""
import b24
import ms
import sys
from settings import B24_LAST_LEADS_LIMIT_DEL


def main():
    res_list = b24.get_last_leads(B24_LAST_LEADS_LIMIT_DEL)
    for lead_id in res_list:
        ms_hash = b24.get_lead_ms_hash(lead_id)
        # print(lead_id, ms_hash, ms.check_ms_order(ms_hash))
        if ms_hash and not ms.check_ms_order(ms_hash):
            print("b24 del lead/ms_hash", lead_id, ms_hash)
            # b24.del_lead(lead_id)
    sys.exit()


if __name__ == "__main__":
    main()