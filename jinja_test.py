import os
import argparse
import paramiko
from scp import SCPClient
from jinja2 import Template
import time

def vpn_conf_geenrator(template_file, list_changes):
    temp = open(template_file).read()
    template = Template(temp)
    output = open(os.path.splitext(template_file)[0] + '.conf', 'w')
    output.write(template.render(certs = list_changes))
    output.close()
    return os.path.splitext(template_file)[0] + '.conf'

test_list = ['controler.crt', 'controller.key' , 'ca.crt']

a = vpn_conf_geenrator('boevoi.j2', test_list)

print(a)