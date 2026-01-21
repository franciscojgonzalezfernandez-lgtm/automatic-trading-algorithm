#
# Interacción con los chats/bots para notificaciones
# Version: 0.1
#
import logging

import telegram_send
from os import path


class Telegram:

    def __init__(self):
        self.__conf_path_1 = path.dirname(path.realpath(__file__)) + "/telegram-bot-group1.conf"
        self.__conf_path_2 = path.dirname(path.realpath(__file__)) + "/telegram-bot-group2.conf"

    def send_message_to_group_1(self, message: str):
        try:
            telegram_send.send(messages=[message], conf=self.__conf_path_1, disable_web_page_preview=True, parse_mode="html")
        except Exception as e:
            logging.exception(repr(e))

    def send_message_to_group_2(self, message: str):
        try:
            telegram_send.send(messages=[message], conf=self.__conf_path_2, disable_web_page_preview=True, parse_mode="html")
        except Exception as e:
            logging.exception(repr(e))