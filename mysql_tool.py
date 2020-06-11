import random
import pymysql
import multiprocessing
import time
import os
import sys

class SQLTools(object):

    def __init__(self, ip, username, password, port, db_name, table_name):
        self.ip = ip
        self.username = username
        self.password = password
        self.port = port
        self.db_name = db_name
        self.table_name = table_name
        self.table_key = []
        self.key_list = ['_num_key', '_name_key', '_time_key']
        self.str_use = 'qwertyuiopasdfghjklzxcvbnm'
        self.status = '1'
    
    def create_table(self, key_num):
        self.status = '1'
        try:
            conn = pymysql.connect(
                host=self.ip,
                port=int(self.port),
                user=self.username,
                password=self.password,
                db=self.db_name,
                charset='utf8'
            )
        except Exception as e:
            self.status = "** Connect error: %s" % str(e).split('\"')[1]
            return

        cur = conn.cursor()
        key_value = {}
        rand_str = ''.join(random.sample(self.str_use, 10))
        key_value['key'] = rand_str + '_id_key'
        rand_num = random.randint(50, 100)
        key_value['type'] = 'int(%s)' % rand_num
        self.table_key.append(key_value)

        key_value = {}
        rand_str = ''.join(random.sample(self.str_use, 10))
        key_value['key'] = rand_str + '_name_key'
        rand_num = random.randint(50, 100)
        key_value['type'] = 'varchar(%s)' % random.randint(50, 100)
        self.table_key.append(key_value)

        key_value = {}
        rand_str = ''.join(random.sample(self.str_use, 10))
        key_value['key'] = rand_str + '_num_key'
        rand_num = random.randint(50, 100)
        key_value['type'] = 'int(%s)' % random.randint(50, 100)
        self.table_key.append(key_value)

        key_value = {}
        rand_str = ''.join(random.sample(self.str_use, 10))
        key_value['key'] = rand_str + '_time_key'
        rand_num = random.randint(50, 100)
        key_value['type'] = 'datetime'
        self.table_key.append(key_value)

        if key_num > 3:
            for i in range(0, key_num - 3):
                key_value = {}
                rand_key = self.key_list[random.randint(0, 2)]
                rand_str = ''.join(random.sample(self.str_use, 10))
                key_value['key'] = rand_str + rand_key
                if 'num' in rand_key:
                    key_value['type'] = 'int(%s)' % random.randint(50, 100)
                
                if 'name' in rand_key:
                    key_value['type'] = 'varchar(%s)' % random.randint(50, 100)
                
                if 'time' in rand_key:
                    key_value['type'] = 'datetime'
                self.table_key.append(key_value)
        
        comm = 'create table %s (%s %s primary key' % (self.table_name, self.table_key[0]['key'], self.table_key[0]['type'])
        for i in self.table_key:
            if '_id_' in i['key']:
                continue
            comm = comm + (',%s %s' % (i['key'], i['type']))
        comm = comm + ');'
        # print(comm)
        try:
            cur.execute(comm)
        except Exception as e:
            self.status = "** %s" % str(e).split('\"')
            return

        conn.commit()
        conn.close()
        cur.close()
        return

    def make_data(self, start_num, end_num):
        conn = pymysql.connect(
            host=self.ip,
            port=int(self.port),
            user=self.username,
            password=self.password,
            db=self.db_name,
            charset='utf8'
        )
        cur = conn.cursor()
        for j in range(start_num, end_num):
            key_comm = self.table_key[0]['key']
            value_comm = '\'%s\'' % j
            for i in range(1, len(self.table_key)):
                key_comm = key_comm + ',%s' % self.table_key[i]['key']
                if '_id_' in self.table_key[i]['key']:
                    value = j
                if 'int' in self.table_key[i]['type']:
                    value = random.randint(0, 100000000)
                if 'varchar' in self.table_key[i]['type']:
                    value = ''.join(random.sample(self.str_use, 20))
                if 'datetime' in self.table_key[i]['type']:
                    year = str(random.randint(1900, 2020))

                    month = random.randint(1, 12)
                    if month < 10:
                        month = '0%s' % month
                    else:
                        month = '%s' % month
                    
                    day = random.randint(1, 28)
                    if day < 10:
                        day = '0%s' % day
                    else:
                        day = '%s' % day

                    value = '%s%s%s' % (year, month, day)
                value_comm = value_comm + ',\'%s\'' % value
            comm = 'insert into %s (%s) values (%s)' % (self.table_name, key_comm, value_comm)
            cur.execute(comm)
        conn.commit()
        conn.close()
        cur.close()

    def insert_data(self, num):
        make_num = int(num / 10)
        start_num = 0
        end_num = make_num
        p1 = multiprocessing.Process(target=self.make_data, args=(start_num, end_num))

        start_num = end_num
        end_num = end_num + make_num
        p2 = multiprocessing.Process(target=self.make_data, args=(start_num, end_num))

        start_num = end_num
        end_num = end_num + make_num
        p3 = multiprocessing.Process(target=self.make_data, args=(start_num, end_num))

        start_num = end_num
        end_num = end_num + make_num
        p4 = multiprocessing.Process(target=self.make_data, args=(start_num, end_num))

        start_num = end_num
        end_num = end_num + make_num
        p5 = multiprocessing.Process(target=self.make_data, args=(start_num, end_num))

        start_num = end_num
        end_num = end_num + make_num
        p6 = multiprocessing.Process(target=self.make_data, args=(start_num, end_num))

        start_num = end_num
        end_num = end_num + make_num
        p7 = multiprocessing.Process(target=self.make_data, args=(start_num, end_num))

        start_num = end_num
        end_num = end_num + make_num
        p8 = multiprocessing.Process(target=self.make_data, args=(start_num, end_num))

        start_num = end_num
        end_num = end_num + make_num
        p9 = multiprocessing.Process(target=self.make_data, args=(start_num, end_num))

        start_num = end_num
        end_num = end_num + make_num
        p10 = multiprocessing.Process(target=self.make_data, args=(start_num, end_num))

        p1.start()
        p2.start()
        p3.start()
        p4.start()
        p5.start()
        p6.start()
        p7.start()
        p8.start()
        p9.start()
        p10.start()

        p1.join()
        p2.join()
        p3.join()
        p4.join()
        p5.join()
        p6.join()
        p7.join()
        p8.join()
        p9.join()
        p10.join()

    def delete(self):
        pass

def read_ini(ini_file):
    ini_obj = {}
    with open(ini_file, 'r') as f:
        ini_info = f.readlines()

    for i in ini_info:
        ini_line = i.replace('\n', '')
        if ';' in ini_line or '#' in ini_line:
            continue

        if '[' in ini_line and ']' in ini_line:
            title = ini_line.replace('[', '').replace(']', '')
            ini_obj[title] = {}
            continue

        if '=' in ini_line:
            key = ini_line.replace(' ', '').split('=')[0]
            value = ini_line.replace(' ', '').split('=')[1]
            if title in ini_obj:
                ini_obj[title][key] = value

    return ini_obj

if __name__ == "__main__":
    multiprocessing.freeze_support()
    path = os.path.dirname(sys.argv[0])
    if 'config.ini' in os.listdir(path):
        try:
            ini_obj = read_ini('%s/config.ini' % path)
        except:
            print('[ERROR] read config.ini error')
            os.system('pause')
            exit()
        else:

            ip = ini_obj['config']['ip']
            username = ini_obj['config']['username']
            password = ini_obj['config']['password']
            port = int(ini_obj['config']['port'])
            db_name = ini_obj['config']['db_name']
            table_name = ini_obj['config']['table_name']
            table_key_num = int(ini_obj['config']['table_key_num'])
            insert_data_num = int(ini_obj['config']['insert_data_num'])
    # print(ini_obj)
    sql_obj = SQLTools(ip, username, password, port, db_name, table_name)
    sql_obj.create_table(table_key_num)
    if sql_obj.status != '1':
        print('[ERROR] %s' % sql_obj.status)
        os.system('pause')
        exit()

    print(time.asctime(time.localtime(time.time())))
    sql_obj.insert_data(insert_data_num)
    print(time.asctime(time.localtime(time.time()) ))
    os.system('pause')