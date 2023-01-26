import os
import sys
import argparse
from datetime import datetime
from time import sleep
from time import time
from io import BytesIO
from threading import Thread

from smb.base import *
from smb.smb_structs import *
from smb.SMBConnection import SMBConnection

IS_DIRECT_TCP = True
CNXN_PORT = 139 if IS_DIRECT_TCP is False else 445
STATUS_LINE = ''

parser = argparse.ArgumentParser(
    prog='SMB Downloader',
    description='Synchronize files through smb by chunks',
    epilog='(c) Meb'
)

parser.add_argument('-t', '--target', required=True, help="Format: pure-ip (192.168.0.1)")
parser.add_argument('-c', '--credential', required=True, help="Format: Login:Password")
parser.add_argument('-f', '--filter', choices=[0, 1], default=0, type=int, help="Filter or not files. Default: 0")
parser.add_argument('-w', '--create', default='01.01.1970', help='Format: DD.MM.YYYY')
parser.add_argument('-s', '--share', required=True, default='C$', help='Share name')
parser.add_argument('-p', '--path', required=False, default='', help='Path to single file for downloading')

args = parser.parse_args()


def output():
    global STATUS_LINE
    while True:
        print('\x1b[2K{}\r'.format(STATUS_LINE), end='', flush=True)
        sleep(0.1)


Thread(target=output, daemon=True).start()


class Connection:
    user_name = ''
    passwd = ''
    ip = ''
    prot = None

    status = False
    samba = None

    r3YrZ28SmQ = []
    last = None

    def __init__(self, user_name, passwd, ip, port=CNXN_PORT):
        global STATUS_LINE
        STATUS_LINE = f'user: {user_name}, pass: {passwd}, ip: {ip}, port: {port}'
        self.user_name = user_name
        self.passwd = passwd
        self.ip = ip
        self.port = port
        self.timer_lap = time.time()
        self.client = ''  # socket.gethostname()
        self.connect()

    def connect(self):
        try:
            self.samba = SMBConnection(self.user_name, self.passwd, self.client, self.ip,
                                       sign_options=SMBConnection.SIGN_NEVER, use_ntlm_v2=False,
                                       is_direct_tcp=IS_DIRECT_TCP)
            self.samba.connect(self.ip, self.port)
            self.status = self.samba.auth_result
            STATUS_LINE = f'[STATUS] {self.status}'
        except Exception as e:
            self.status = False
            self.samba.close()
            STATUS_LINE = '[ERROR]' + e.__str__()
            # sleep(10)

        return self.status

    def disconnect(self):
        self.samba.close()

    def tree(self, shared_device, top, single=''):
        global STATUS_LINE

        if single:
            e = self.samba.getAttributes(shared_device, single)

            single = single.replace('\\', '/')
            try:
                our_size = os.path.getsize('./' + single)
            except:
                our_size = 0

            try:
                existed = os.path.exists('./' + single)
            except:
                existed = False

            if not existed or our_size != e.file_size:
                single = single.replace('\n', '')
                self.r3YrZ28SmQ.append(single)
                self.download_by_path_chunks(shared_device, offset=our_size, total_size=e.file_size)

            return

        dirs = []
        # returns https://pysmb.readthedocs.io/en/latest/api/smb_SharedFile.html
        try:
            alls = self.samba.listPath(shared_device, top)
        except (NotConnectedError, SMBTimeout, NotReadyError) as e:
            self.disconnect()
            flag = self.connect()
            while not flag:
                sleep(10)
                flag = self.connect()

            return self.tree(shared_device, top, single=single)

        for e in alls:
            if e.isDirectory:
                if e.filename not in [u'.', u'..']:
                    dirs.append(e.filename)
            else:
                if e.last_write_time < datetime.strptime(args.create, '%d.%m.%Y').timestamp():
                    continue

                extensions = ['.mp4', '.jpg', 'vpn', '.jpeg', '.png', '.bmp', '.odt', '.doc', '.docx', '.rtf', '.xls',
                              '.xlsx', '.pdf', '.rar', '.zip', '.7z', '.txt']
                flag = False if args.filter else True
                for ext in extensions:
                    if ext in e.filename:
                        flag = True

                if not flag:
                    STATUS_LINE = 'Skipped\t' + e.filename
                    continue

                if e.filename == 'Thumbs.db':
                    continue

                file_path = os.path.join(top, e.filename)

                try:
                    existed = os.path.exists('./' + file_path)
                except:
                    existed = False

                try:
                    our_size = os.path.getsize('./' + file_path)
                except:
                    our_size = 0

                if not existed or our_size != e.file_size:
                    file_path = file_path.replace('\n', '')
                    self.r3YrZ28SmQ.append(file_path)
                    self.download_by_path_chunks(shared_device, offset=our_size, total_size=e.file_size)
                else:
                    STATUS_LINE = 'Skipped\t' + file_path

        for d in dirs:
            try:
                if d:
                    nd = os.path.join(top, d)
                    result = self.tree(shared_device, nd, single=single)
                    dirs.extend(result)
                    dirs = list(set(dirs))
            except:
                continue

        return dirs

    def download_by_path_chunks(self, shared_device, offset=0, total_size=0):
        global STATUS_LINE
        obj = self.r3YrZ28SmQ[0]
        try:
            try:
                path_for_creating = f'./' + '/'.join(obj.split('/')[:-1])
                os.makedirs(path_for_creating)
            except Exception as e:
                pass

            path_for_download = f'./' + obj
            path_for_download = path_for_download.replace('\n', '')
            file_obj = BytesIO()
            f = open(path_for_download, 'ab')

            while True:
                try:
                    result = self.samba.retrieveFileFromOffset(shared_device, obj, f, offset=offset,
                                                               max_length=1024 * 400)

                    file_obj.seek(offset)

                    for line in file_obj:
                        fw.write(line)

                    offset = offset + result[1]

                    if result[1] == 0:
                        STATUS_LINE = f'Done [{path_for_download}]'
                        self.r3YrZ28SmQ.pop(0) if len(self.r3YrZ28SmQ) > 0 else None
                        return

                    STATUS_LINE = f'Downloading\t{offset} | {total_size} bytes\t[{path_for_download}]'
                except (NotConnectedError, SMBTimeout, NotReadyError) as e:
                    self.disconnect()
                    flag = self.connect()
                    while not flag:
                        sleep(10)
                        flag = self.connect()
                    continue

            sys.stdout.write('\n')
            self.r3YrZ28SmQ.pop(0)
        except (NotConnectedError, SMBTimeout, NotReadyError) as e:
            self.disconnect()
            flag = self.connect()
            while not flag:
                sleep(10)
                flag = self.connect()

            self.download_by_path_chunks(shared_device, offset=offset, total_size=total_size)
        except OperationFailure as e:
            STATUS_LINE = 'Unable to open remote for reading [' + path_for_download + ']'
            self.r3YrZ28SmQ.pop(0)
        except Exception as e:
            STATUS_LINE = '[Failed] Unexcepted error' + e.__str__()


if __name__ == '__main__':
    sys.setrecursionlimit(1500 * 20)
    probs = {args.credential: ['']}

    for k, v in probs.items():
        try:
            log, pw = k.split(':')[0], k.split(':')[1]

            if probs.get(k, 'Invalid user') == 'Invalid user':
                continue
            tmpd = probs.get(k, ['/'])

            s = Connection(log, pw, args.target)

            STATUS_LINE = f'[AUTH] {s.samba.has_authenticated}'
            STATUS_LINE = f'[NEG SESSION] {s.samba.has_negotiated}'
            STATUS_LINE = f'[TREES] {s.samba.connected_trees}'
            for d in tmpd:
                s.tree(args.share, f'{d}', single=args.path)
        except Exception as e:
            STATUS_LINE = '[MAIN ERROR]' + e.__str__()
            s.disconnect()
        finally:
            s.disconnect()
