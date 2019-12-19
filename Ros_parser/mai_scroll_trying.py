from selenium import webdriver
import selenium.common.exceptions
from selenium.webdriver.common.action_chains import ActionChains
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
        self.wait_short = WebDriverWait(self.browser, 5)

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
                show_more_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="results"]/form/button')))
                show_more_button.click()
        except (selenium.common.exceptions.StaleElementReferenceException, selenium.common.exceptions.TimeoutException) as error:
            logging.error("Не удалось нажать кнопку 'показать еще':{}".format(error))
            logging.info("Обновляю страницу, запускаю повторный цикл")
            self.browser.get("https://roscongress.org/news/")
        # self.wait.until(EC.element_to_be_clickable((By.XPATH, "//div[@class='filters-list']/div")))
        logging.info('Кнопка "показать еще" была нажата {} раз'.format(count_clicks))

    def scroll_to_element(self, iter_count):
        logging.info("Скроллим к след элементу")
        try:
            for i in range(iter_count):
                self.wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="results"]/form/button')))
                actions = ActionChains(self.browser)
                actions.move_to_element((self.browser.find_element_by_xpath('//*[@id="results"]/form/button'))).perform()
        except (selenium.common.exceptions.StaleElementReferenceException, selenium.common.exceptions.TimeoutException):
            logging.error("Обновляю страницу, запускаю повторный цикл")
            self.browser.get("https://roscongress.org/news/")
        # Возможно тут имеет смысл ждать загрузки элемента, а не всего блока или вообще ничего не ждать
        # self.wait.until(EC.presence_of_all_elements_located((By.XPATH, "//div[@class='filters-list']/div")))
        self.browser.execute_script("window.scrollTo(0, 0)")


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

# Нацинаем цикл для парсинга новостей
Count_of_iteration = 1
Count_of_iteration_error = Count_of_iteration
while True:
    # Тут можно конечно не два раза перезагружать страницу, а просто отслеживать если цикл закончился на статье которая
    # есть в БД и только тогда перезагружать
    Browser.browser.get("https://roscongress.org/news/")
    logging.info("Старт цикла {}".format(Count_of_iteration))
    if Count_of_iteration > 1:
        Browser.scroll_to_element(Count_of_iteration - 1)

    # Получаем множество набора статей набор статей
    Browser.wait.until(EC.presence_of_all_elements_located((By.XPATH, "//div[@class='filters-list']/div")))
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

    # Считаем количество ошибок в цикле и увеличиваем обновление страницы на количество ошибок
    while_iter_error_count = 0
    for i in SetNew - SetOld:
        while_iter = 1
        while True:
            if while_iter > 100:
                logging.critical("Не удалось открыть страницу")
                exit(1)
            try:
                Browser.wait.until(EC.presence_of_element_located((By.ID, i)))
            except (selenium.common.exceptions.TimeoutException, selenium.common.exceptions.NoSuchElementException) as error:
                logging.error("Следующая новость в цикле не найдена, перезагружаем страницу, попытка {} из 100".format(
                    while_iter))
                Browser.browser.get("https://roscongress.org/news/")
                while_iter += 1
                while_iter_error_count += 1
                if Count_of_iteration > 1:
                    # Если мы не смогли найти новость обновив стараницу n раз увеличиваем это число для итерации
                    Count_of_iteration_error = while_iter_error_count + Count_of_iteration
                    # Скороллим по новой
                    Browser.scroll_to_element(Count_of_iteration_error)
                continue
            break

        # находим элемент страницы в БД
        current_elem = Browser.browser.find_element_by_id(i)
        current_elem = current_elem.find_element_by_class_name("title").text
        if DB.check(current_elem):
            News = Browser.parse_news(i)
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
        else:
            logging.info("Новость c заголовском({}) уже есть в БД".format(current_elem))
            continue

        if Count_of_iteration > 1:
            Browser.scroll_to_element(Count_of_iteration_error)

    # Обновляем мнодество id новостей
    del SetOld
    SetOld = SetNew.copy()
    logging.info("Цикл {} Закончен".format(Count_of_iteration))
    Count_of_iteration += 1

Browser.browser.close()
