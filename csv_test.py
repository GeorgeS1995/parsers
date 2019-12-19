import csv


def csv_reader(file_obj):
    """
    Read a csv file
    """
    reader = csv.reader(file_obj)
    for row in reader:
        print(" ".join(row))


def csv_writer(data, path):
    """
    Write data to a CSV file path
    """
    with open(path, "a", newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=';')
        for line in data:
            writer.writerow(line)

data = ['[eq;gbplf;l;buehlf'.split(';'),
    'хуй;пиздай;джигурда'.split(';')]
csv_writer(data,'test_csv.csv')

with open('test_csv.csv', "r") as f_obj:
    csv_reader(f_obj)

