from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import selenium.common.exceptions
import os
import logging
import psycopg2
import sys
import re


class BrowserHandler:

    selectLinkOpeninNewTab = Keys.LEFT_CONTROL + 't'
    CloseTab = Keys.LEFT_CONTROL + 'w'

    def __init__(self):
        # Почему он просто не захотел видеть, что драйвер в папке с скриптов ХЗ
        self.browser = webdriver.Chrome(os.getcwd() + '/chromedriver')
        self.wait = WebDriverWait(self.browser, 15)
        self.short_wait = WebDriverWait(self.browser, 1)

    def program_parser(self, url):
        # Список тэгов, с помощью него я потмо буду в цикле связывать тэги и факты
        tag_list = []
        logging.info("start parse link: {}".format(url))
        # Запоминаю первую вкладку
        curWindowHndl = self.browser.current_window_handle
        self.browser.execute_script('''window.open("{}", "_blank");'''.format(url))
        self.browser.switch_to.window(self.browser.window_handles[1])
        # блок работы с страницей
        try:
            self.wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[3]/div/div[3]/div/div[1]")))
            programm_block = self.browser.find_element(By.XPATH, "/html/body/div[3]/div/div[3]/div/div[1]")
            programm_block = programm_block.find_elements(By.CLASS_NAME, "session")

            # Запоминаем название программы, так как она тоже тэг
            programm_tag = self.browser.find_element(By.CSS_SELECTOR, "body > div.content-wrapper > div > div.site-nav > a.site-nav__link.site-nav__link--active").text
            # Игорь попросил генерировать такое для тэгов
            programm_tag = DB.add_tag(programm_tag, 1)
            tag_list.append(programm_tag[0])
            for i in programm_block:
                logging.info("start parse block id: {}".format(i.get_attribute("id")))
                # Считываем теги, возможно это можно делать не ожидая шибку
                try:
                    self.short_wait.until(EC.presence_of_element_located(i.find_element(By.CLASS_NAME, "activity")))
                    activity_tag = DB.add_tag(i.find_element(By.CLASS_NAME, "activity").text, 3)
                    tag_list.append(activity_tag[0])
                except (selenium.common.exceptions.TimeoutException, selenium.common.exceptions.NoSuchElementException):
                    logging.warning('тэг "Тип" не найден для блока')
                try:
                    self.short_wait.until(EC.presence_of_element_located(i.find_element(By.CLASS_NAME, "session__subject")))
                    direction_tag = DB.add_tag(i.find_element(By.CLASS_NAME, "session__subject").text, 2)
                    tag_list.append(direction_tag[0])
                except (selenium.common.exceptions.TimeoutException, selenium.common.exceptions.NoSuchElementException):
                    logging.warning('тэг "Направление" не найден для блока')
                # # Записываем место проведения в БД
                # place =  i.find_element_by_class_name("session__info-text").text
                # place_insert = DB.check_insert_query("insert into public.places (eventid,description) values (172,'{}') returning placeid;".format(place), {'description'})
                # # Записываем
                # print(place_insert[0])
                # print(i.find_element_by_class_name("session__info-date").text)

            logging.info("page: {} parsed".format(url))
        except (selenium.common.exceptions.TimeoutException, selenium.common.exceptions.NoSuchElementException):
            logging.warning("url не содержит программы")
        self.browser.close()  # closes new tab
        self.browser.switch_to.window(curWindowHndl)




class DBhandler:
    def __init__(self):
        # Подключаемся к БД
        try:
            self.connection = psycopg2.connect(user="admin",
                                               password="d2309-0er8f-u3498",
                                               host="91.107.84.69",
                                               port="43344",
                                               database="RK5")
            self.cursor = self.connection.cursor()

            # Print PostgreSQL Connection properties
            logging.info("Подключение к БД установленно".format(self.connection.get_dsn_parameters()))
            # Print PostgreSQL version
            self.cursor.execute("SELECT version();")
        except (Exception, psycopg2.Error) as error:
            logging.critical("Не удалось поключиться к БД: {}".format(error))
            Browser.browser.close()
            exit(1)

    def add_tag(self, tag_name, tag_type):
        # Отдельная функция для добавления тегов, нужна из-за неработающего аутоинкремента
        self.cursor.execute("select * from public.tag where \"name\" = '{}';".format(tag_name))
        result = self.cursor.fetchall()
        if result == []:
            try:
                tag_perm = self.perm_gen()
                self.cursor.execute("insert into public.tag (eventid,sortpriority,\"name\",permissionid,tagtypeid) values (172,0,'{}',{},{}) returning tagid;".format(tag_name,tag_perm,tag_type))
                self.connection.commit()
                logging.info("Данные добавлены в БД")
                return self.cursor.fetchone()
            except (Exception, psycopg2.Error) as error:
                logging.error("Не удалось записать данные в БД: {}".format(error))
        logging.warning("Данные уже есть в БД")
        return result[0]

    def perm_gen(self):
        # Функция существует исключительно потому что автоинкремент в таблице пермишено работает неправильно
        self.cursor.execute("select * from permissions order by permissionid desc limit 1;")
        result = self.cursor.fetchall()
        self.cursor.execute("insert into public.permissions (permissionid) values ({}) returning permissionid;".format(result[0][0] + 1))
        return self.cursor.fetchone()[0]

    def check_insert_query(self, query, fields):
        try:
            query_table_name = query[query.index("into ") + len("into "):query.index("(")]
            query_table_fields = query[query.index("(") + 1:query.index(")")].split(",")
            # Сложая скопипащеная регулярка
            query_table_insert_value = re.split(''',(?=(?:[^'"]|'[^']*'|"[^"]*")*$)''', query[query.index("values (") + 8:query.rindex(")")])
            for i, v in enumerate(query_table_fields):
                if v.strip() in fields:
                    self.cursor.execute("select * from {} where {} = {};".format(query_table_name, v.strip(),
                                                                                         query_table_insert_value[i]))
                    result = self.cursor.fetchall()
                    if result != []:
                        logging.warning("Данные уже есть в БД")
                        return result[0]
            try:
                self.cursor.execute(query)
                self.connection.commit()
                logging.info("Данные добавлены в БД")
                return self.cursor.fetchone()
            except (Exception, psycopg2.Error) as error:
                logging.error("Не удалось записать данные в БД: {}".format(error))
        except ValueError:
            logging.critical("Неправильный sql запрос")
            exit(1)

# Создаем папку для логирования и складываем логи запусков
# Nbgj nen rjl rjnjhsq jrulf-yb,elm ,eltn
logging.basicConfig(format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', filemode="w",
                    filename="VEF_parser.log", level=logging.INFO)

root = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

logging.info("Starting parser")
# Задаем по экземпляру селениума и БД
Browser = BrowserHandler()
DB = DBhandler()

# Главная ВЭФ
try:
    Browser.browser.get("https://forumvostok.ru/")
except:
    logging.critical("Не удалось открыть главную ВЭФ")
    Browser.browser.close()
    exit(1)

programms = Browser.browser.find_elements_by_xpath("/html/body/header/div[3]/div[2]/div[2]/div[1]/div[3]/ul/li")

# Получаем список всех программ
for i in programms:
    Browser.program_parser(i.find_element_by_class_name("dropdown__link").get_attribute("href"))


logging.info("Парсинг окончен")
Browser.browser.close()