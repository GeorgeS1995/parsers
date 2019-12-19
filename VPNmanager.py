import os
import argparse
import paramiko
from scp import SCPClient
from jinja2 import Template
import warnings
import time
import csv

# создать класс проверок
    # Проверка выбрала ли file_sort ровно 2 файла
    # Проверка все ли файлы существуют
    # Проверку валидности конфига
def file_parser(file):
    ''' Парсит файл с входа в формате Key = keyvalue

    :return Словарь формата {'Key' : ' keyvalue'}'''
    output_dict = {}
    f = open(file)
    for line in f:
        output_dict[line[0:line.find(' ')]] = line[line.find(' ') + 1: -1]
    return output_dict


class File_handler:
    def file_sort(self, files_path, file_temp, ext):
        # добавляет в кортеж абсолютные пути к файлам с указнным расширением и содержашие в названии шаблон
        output = []
        for roots, dirs, files in os.walk(os.path.normpath(files_path)):
            for file in files:
                for i in ext:
                    if file.find(file_temp) != -1 and os.path.splitext(os.path.join(roots,file))[1] == i:
                        output.append(os.path.join(roots, file))
        return output

    def ssh_connect(self, host, user, password, port):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # подавление предупрежедний парамико ибо он заебла
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            client.connect(hostname=host, username=user, password=password, port=port)
        return client

    def file_sender(self, local_path, remote_path, client):
        scp = SCPClient(client.get_transport())
        scp.put(local_path, remote_path=remote_path)
        scp.close()

    def vpn_conf_generator(self, template_file, list_changes):
        dict_changes = self.dict_gen(list_changes)
        temp = open(template_file).read()
        template = Template(temp)
        output = open(os.path.splitext(template_file)[0] + '.conf', 'w')
        output.write(template.render(ca=dict_changes['ca'], cert=dict_changes['cert'], key=dict_changes['key']))
        output.close()
        return os.path.splitext(template_file)[0] + '.conf'

    def dict_gen(self, list_changes):
        '''подготавливает словарь для vpn_conf_generator'''
        output_dict = {}
        for i in list_changes:
            if i == 'ca.crt':
                output_dict['ca'] = i
            elif os.path.splitext(i)[1] == '.key':
                output_dict['key'] = i
            else:
                output_dict['cert'] = i
        return output_dict


class CVS_handler:
    def __init__(self, connect):
        self.connect = connect

    def hostname_definition(self):
        sdtin, stdout, stderr = self.connect.exec_command('hostname')
        return stdout.read().decode("utf-8")

    def mac_definition(self):
        sdtin, stdout, stderr = self.connect.exec_command("ifconfig -a | grep ether | gawk '{print $2}' | head -1")
        return stdout.read().decode("utf-8")

    def teamviewer_definition(self):
        sdtin, stdout, stderr = self.connect.exec_command("teamviewer -info | grep 'TeamViewer ID:' | awk '{print $5}'")
        return stdout.read().decode("utf-8")

    def version_of_service(self):
        sdtin, stdout, stderr = self.connect.exec_command("ls -l /home/user/ | grep Uniteller | awk '{print $9}'")
        product = stdout.read().decode("utf-8")
        sdtin, stdout, stderr = self.connect.exec_command("cd /home/user/{} ; ./version.sh | grep Linux | awk '{{print $3}}'".format(product[:-1]))
        version = stdout.read().decode("utf-8")
        return product, version

    def upid_definition(self):
        product, version = self.version_of_service()
        sdtin, stdout, stderr = self.connect.exec_command("cat /home/user/{}/service.config | grep Identification | awk '{{print $3}}'".format(product[:-1]))
        return stdout.read().decode("utf-8")

    def csv_writer(self, data, path):
        """
        Write data to a CSV file path
        """
        with open(path, "a", newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=';')
            for line in data:
                writer.writerow(line)


# Обязательные и возможные аргументы
parse = argparse.ArgumentParser()

parse.add_argument('-a', '--address', required=True, help='Target machine IP address')
parse.add_argument('-c', '--controller', help='Controller VPN number')
parse.add_argument('-b', '--boevoi', help='boevoi VPN')
parse.add_argument('-r', '--reload', nargs='?', const='exist', help='reload VVP on controller')
parse.add_argument('-t', '--test', nargs='?', const='exist', help='reload VVP on controller')


# набор переменных для подключения и копирования
Sender = File_handler()
config = file_parser('VPNmanager.conf')
connection = Sender.ssh_connect(parse.parse_args().address, config['SSH_login'], config['SSH_pass'], '22')
ext = ['.crt', '.key']

# Действия на каждый аргумент
# Надо это сделать не так топорно, но я пока не понимаю как
if parse.parse_args().controller != None:
    #try:
        controllers_files = Sender.file_sort(config['Controller_VPNfiles_dir'], parse.parse_args().controller, ext)
        controllers_files.append(config['Controller_VPNca_dir'])
        # Маленький цикл подготовки списка для функции vpn_conf_generator
        input_list = []
        for i in controllers_files:
            input_list.append(os.path.split(i)[1])
        connection.exec_command('mkdir -p /etc/openvpn/ctrl')
        # отправляем комплект сертификатов
        for i in controllers_files:
            Sender.file_sender(i, '/etc/openvpn/ctrl', connection)
        Sender.file_sender(Sender.vpn_conf_generator('ctrl.j2', input_list), '/etc/openvpn', connection)
        print('Files successfully sent:\n {}\n'.format(controllers_files))
        stdin, stdout, stderr = connection.exec_command('systemctl enable openvpn@ctrl')
        print(stderr.read().decode("utf-8"))
        stdin, stdout, stderr = connection.exec_command('systemctl start openvpn@ctrl')
        print(stderr.read().decode("utf-8"))
        time.sleep(5)
        stdin, stdout, stderr = connection.exec_command('ifconfig | grep -A 7 tap')
        print(stdout.read().decode("utf-8") + stderr.read().decode("utf-8"))
        # Удаляем уже пересланный конфиг
        os.remove('ctrl.conf')
   # except Exception:
    #    print('Something went wrong')
if parse.parse_args().boevoi != None:
     # try:
        controllers_files = Sender.file_sort(config['Boevoi_VPNfiles_dir'], parse.parse_args().boevoi, ext)
        controllers_files.append(config['Boevoi_VPNca_dir'])
        # Маленький цикл подготовки списка для функции vpn_conf_generator
        input_list = []
        for i in controllers_files:
            input_list.append(os.path.split(i)[1])
        connection.exec_command('mkdir -p /etc/openvpn/boevoi')
        # отправляем комплект сертификатов
        for i in controllers_files:
            Sender.file_sender(i, '/etc/openvpn/boevoi/', connection)
        Sender.file_sender(Sender.vpn_conf_generator('boevoi.j2', input_list), '/etc/openvpn', connection)
        print('Files successfully sent:\n {}\n'.format(controllers_files))
        stdin, stdout, stderr = connection.exec_command('systemctl enable openvpn@boevoi')
        print(stderr.read().decode("utf-8"))
        stdin, stdout, stderr = connection.exec_command('systemctl start openvpn@boevoi')
        print(stderr.read().decode("utf-8"))
        time.sleep(5)
        stdin, stdout, stderr = connection.exec_command('ifconfig | grep -A 7 tap')
        print(stdout.read().decode("utf-8") + stderr.read().decode("utf-8"))
        # Удаляем уже пересланный конфиг
        os.remove('boevoi.conf')
     # except Exception:
        # print('Something went wrong')
if parse.parse_args().reload != None:
    # Запоминаю откуда запускался скрипт
    pwd = os.getcwd()
    parsed_VPNconf = file_parser(config['Keyload_path'])
    loadkey_par_list = [parsed_VPNconf['ca'], parsed_VPNconf['cert'], parsed_VPNconf['key']]
    loadkey_for_template = []
    for i in loadkey_par_list:
            kostil = os.path.basename(i).replace("'", "")
            loadkey_for_template.append(kostil)
    Sender.file_sender(Sender.vpn_conf_generator('keyload.j2', loadkey_for_template), '/etc/openvpn', connection)
    # Переходим в директорию vpn конфига, тк как пути в конфиге могут быть не абсоюлтными
    os.chdir(os.path.split(config['Keyload_path'])[0])
    for i in loadkey_par_list:
        Sender.file_sender(os.path.normpath(i.replace("'", "")), '/etc/openvpn', connection)
    os.chdir(pwd)
    loadkey_for_template.append('keyload.conf')
    print('Files successfully sent:\n {}\n'.format(loadkey_for_template))
    stdin, stdout, stderr = connection.exec_command('systemctl start openvpn@keyload')
    print(stderr.read().decode("utf-8"))
    time.sleep(5)
    stdin, stdout, stderr = connection.exec_command('ifconfig | grep -A 7 tap')
    print(stdout.read().decode("utf-8") + stderr.read().decode("utf-8"))
    # Активируем loadkey.sh
    stdin, stdout, stderr = connection.exec_command('find /home/user/ | grep loadkeys.sh | sh')
    print(stdout.read().decode("windows-1251") + stderr.read().decode("windows-1251"))
    # Закрываем keyload
    stdin, stdout, stderr = connection.exec_command('systemctl stop openvpn@keyload')
    print(stderr.read().decode("utf-8"))
    time.sleep(1)
    stdin, stdout, stderr = connection.exec_command('ifconfig | grep -A 7 tap')
    print(stdout.read().decode("utf-8") + stderr.read().decode("utf-8"))
    for i in loadkey_for_template:
        stdin, stdout, stderr = connection.exec_command('rm -f /etc/openvpn/{}'.format(i))
        print(stdout.read().decode("utf-8") + stderr.read().decode("utf-8"))
    os.remove('keyload.conf')
if parse.parse_args().test != None:
    CVS = CVS_handler(connection)
    print(CVS.hostname_definition())
    print(CVS.mac_definition())
    print(CVS.teamviewer_definition())
    product, version = CVS.version_of_service()
    print(product)
    print(version)
    print(CVS.upid_definition())
    test = ['ca.crt', 'test.crt', 'test.key']
    Sender.vpn_conf_generator('ctrl.j2', test)

CVS_data = CVS_handler(connection)
date = time.strftime('%d.%m.%Y')
product, version = CVS_data.version_of_service()
if os.path.exists(config['CSV_path']) == True and (parse.parse_args().boevoi != None or parse.parse_args().controller != None):
    write_data = [[date, CVS_data.hostname_definition(), CVS_data.mac_definition(), CVS_data.teamviewer_definition(),
                  parse.parse_args().controller, parse.parse_args().boevoi, product, version, CVS_data.upid_definition()], ]
    CVS_data.csv_writer(write_data, config['CSV_path'])
elif parse.parse_args().boevoi != None or parse.parse_args().controller != None:
    head = 'Date;Hostname;MAC;Teaviewer;ctrl_cert;boy_certs;Server;Version;UPID'.split(';')
    write_data = [head, [date, CVS_data.hostname_definition(), CVS_data.mac_definition(), CVS_data.teamviewer_definition(),
                  parse.parse_args().controller, parse.parse_args().boevoi, product, version, CVS_data.upid_definition()]]
    CVS_data.csv_writer(write_data, config['CSV_path'])
