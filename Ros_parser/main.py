from selenium import webdriver
import selenium.common.exceptions
from selenium.webdriver.common.keys import Keys
import os
import logging
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import psycopg2
import sys
import time
import datetime


class BrowserHandler:
    def __init__(self):
        # Почему он просто не захотел видеть, что драйвер в папке с скриптов ХЗ
        self.browser = webdriver.Chrome(os.getcwd() + '/chromedriver')
        self.wait = WebDriverWait(self.browser, 60)

    def parse_news(self, div_id):
        output_dict = {}
        text = str()
        current_div = self.browser.find_element_by_id(div_id)
        news_link = current_div.find_element_by_class_name("title")
        output_dict["link"] = news_link.get_attribute("href")
        logging.info("Парсим новость по адресу {}".format(news_link.get_attribute("href")))
        self.browser.get(news_link.get_attribute("href"))

        # Получаем заголовок новости
        news_label = self.browser.find_element_by_xpath(
            "/html/body/div[3]/div[1]/div/div[2]/div/div[2]/div[1]/div[2]/div")
        output_dict["label"] = news_label.text

        # Получаем время новости
        news_public_date = self.browser.find_element_by_xpath(
            "/html/body/div[3]/div[1]/div/div[2]/div/div[2]/div[1]/div[1]/div/div[2]")
        output_dict["time"] = datetime.datetime.strptime(news_public_date.text, "%d.%m.%Y").strftime("%Y-%m-%d %H:%M:%S")

        # получаем текст новости
        news_text = self.browser.find_elements_by_xpath("/html/body/div[3]/div[1]/div/div[2]/div/div[3]/div[1]/div/div")
        for i in news_text:
            text = text + i.text
        output_dict["text"] = text

        # Получаем url кратинки новости
        news_image_url = self.browser.find_element_by_xpath("/html/body/div[3]/div[1]/div/div[2]/div/div[3]/div[2]/div")
        news_image_url = news_image_url.find_element_by_tag_name("img").get_attribute("src")
        output_dict["image"] = news_image_url
        logging.info("Парсинг окончен")
        self.browser.get("https://roscongress.org/news/")
        return output_dict

    def click_show_more(self, count_clicks):
        # Кликаем необходимое количество раз на кнопку показать еще
        try:
            for i in range(count_clicks):
                show_more_button = Browser.wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="results"]/form/button')))
                show_more_button.click()
        except (selenium.common.exceptions.StaleElementReferenceException, selenium.common.exceptions.TimeoutException) as error:
            logging.error("Не удалось нажать кнопку 'показать еще':{}".format(error))
            logging.info("Обновляю страницу, запускаю повторный цикл")
            self.browser.get("https://roscongress.org/news/")
        Browser.wait.until(EC.element_to_be_clickable((By.XPATH, "//div[@class='filters-list']/div")))
        logging.info('Кнопка "показать еще" была нажата {} раз'.format(count_clicks))


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

    def check(self, title):
        # Проверяем существует ли новость в БД, по заголовку
        self.cursor.execute("select * from articles where title = '{}';".format(title))
        result = self.cursor.fetchall()
        if result == []:
            return True
        logging.info("Новость уже есть в БД")
        return False



# Создаем папку для логирования и складываем логи запусков
# Nbgj nen rjl rjnjhsq jrulf-yb,elm ,eltn
logging.basicConfig(format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', filemode="w",
                    filename="Ros_parser.log", level=logging.INFO)

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

# первоначальный сет, с которым будем сравнивать элементы при первом цикле
SetOld = {i for i in range(12)}
logging.info("Множество для первого цикла создано: {}".format(SetOld))

Browser.browser.get("https://roscongress.org/news/")
# Нацинаем цикл для парсинга новостей
Count_of_iteration = 1

while True:
    logging.info("Старт цикла {}".format(Count_of_iteration))
    if Count_of_iteration > 1:
        # кликаем на кнопочку Больше статейб по количеству пройденных циклов
        Browser.click_show_more(Count_of_iteration - 1)

    # Получаем множество набора статей набор статей
    list_news = Browser.browser.find_elements_by_xpath("//div[@class='filters-list']/div")
    SetNew = set()
    for i in list_news:
        SetNew.add(i.get_attribute("id"))
    logging.info("Список новостей успешно получен")

    if SetNew == SetOld:
        logging.info("Новых новостей нет")
        time.sleep(10)
        Browser.browser.get("https://roscongress.org/news/")
        Count_of_iteration += 1
        continue

    for i in SetNew - SetOld:
        # # Ждем пока весь блок новостей загрузится, пытаясь перезагружать страницу если не нашли
        while_iter = 1
        while True:
            if while_iter > 100:
                logging.critical("Не удалось открыть страницу")
                exit(1)
            try:
                Browser.wait.until(EC.element_to_be_clickable((By.ID, i)))
            except selenium.common.exceptions.TimeoutException as error:
                logging.error("Следующая новость в цикле не найдена, перезагружаем страницу, попытка {} из 100".format(while_iter))
                Browser.browser.get("https://roscongress.org/news/")
                if Count_of_iteration > 1:
                    # кликаем на кнопочку Больше статейб по количеству пройденных циклов
                    Browser.click_show_more(Count_of_iteration)
                while_iter += 1
                continue
            break


        News = Browser.parse_news(i)
        if DB.check(News["label"]):
            logging.info("Запись в БД: {}".format(News["link"]))
            try:
                DB.cursor.execute(
                    "INSERT INTO public.articles (title, creationdate, publicdate, shortdesc,description,previewimgurl,mainimgurl,eventid,articletypeid)"
                    " VALUES ('{0}','{4}','{4}','{1}','{2}','{3}','{3}',1, 1);".format(News["label"], News["text"][0:300], News["text"],
                                                                           News["image"], News["time"]))
                DB.connection.commit()
            except(Exception, psycopg2.Error) as error:
                logging.error("Не удалось Записать данные в БД: {}".format(error))
            logging.info("Данные записаны")

        if Count_of_iteration > 1:
            # кликаем на кнопочку Больше статейб по количеству пройденных циклов
            Browser.click_show_more(Count_of_iteration - 1)

    # Обновляем мнодество id новостей
    del SetOld
    SetOld = SetNew.copy()
    logging.info("Цикл {} Закончен".format(Count_of_iteration))
    Count_of_iteration += 1

Browser.browser.close()
