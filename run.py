from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime
import argparse
import os
import paramiko
import pysftp
import decorators
import sqlite3
import common_logging as logging
import utils

logger = logging.getLogger(__name__)


class DirectoryDesc:
    def __init__(self, src, dst) -> None:
        self.src = src
        self.dst = dst

    def __repr__(self) -> str:
        return f'{self.src} => {self.dst}'


def db_conn():
    return sqlite3.connect("download.db")


@contextmanager
def closing_ctx(res):
    try:
        yield res
    finally:
        res.close()


def connect(pem_key_path: str):
    host = "3.9.198.111"
    special_account = "ubuntu"
    pkey = paramiko.RSAKey.from_private_key_file(pem_key_path)
    client = paramiko.SSHClient()
    policy = paramiko.AutoAddPolicy()
    client.set_missing_host_key_policy(policy)
    client.connect(host, username=special_account, pkey=pkey)
    return client


@decorators.timer()
def main(key, base_path: str, dirs: list[DirectoryDesc] = None, date_from=None, date_to=None):
    @decorators.timer()
    def record_download(l_path: str, m_date, category: str, size: float):
        con = db_conn()
        cur = con.cursor()
        sql = f"INSERT INTO downloads (path, mdate, category, size) VALUES ('{l_path}', '{m_date}', '{category}', {size})"
        cur.execute(
            sql).fetchone()
        con.commit()

    @decorators.timer()
    def check_exists(l_path: str) -> bool:
        sql = f"SELECT * FROM downloads WHERE path = '{l_path}'"
        con = db_conn()
        cur = con.cursor()
        res = cur.execute(
            sql).fetchone()
        return True if res else False

    @decorators.timer()
    def download_file(d: DirectoryDesc, sftp, r_path, l_path, m_time, file_size):
        try:
            if not check_exists(l_path):
                sftp.get(r_path, l_path)
                record_download(l_path, m_time, d.src, file_size)
                logger.debug(
                    f'{m_time} - Downloaded: {r_path} => {l_path}/{m_time}')
            else:
                logger.debug(f'{m_time} - Exists: {l_path}')
        except Exception as ex:
            logger.error(f'Exception: {ex}')
            utils.print_stack_trace()

    if dirs:
        host = "3.9.198.111"
        special_account = "ubuntu"
        for d in dirs:
            paths = []
            with closing_ctx(connect(key)) as client:
                with client.open_sftp() as sftp:
                    remote_dirs = sftp.listdir_iter(
                        path=f'{base_path}/{d.src}')
                    for p in remote_dirs:
                        m_time = datetime.fromtimestamp(p.st_mtime).date()
                        file_size = p.st_size/(1024.0*1024.0)  # in MB
                        if m_time >= date_from and m_time <= date_to:
                            r_path = f'{base_path}/{d.src}/{p.filename}'
                            l_path = f'{d.dst}\{p.filename}'
                            paths.append((r_path, l_path, m_time, file_size))

            paths.sort(key=lambda p: p[2], reverse=True)
            from_to = None
            if len(paths):
                from_to = (paths[-1][2], paths[0][2])

            print('\n\n\n------------------------------------')
            logger.debug(f'{d.src} Filtered=> {len(paths)} => {from_to}')

            def download(p):
                with pysftp.Connection(host=host, username=special_account, private_key=key) as sftp:
                    download_file(d, sftp, p[0], p[1], p[2], p[3])

            with ThreadPoolExecutor(max_workers=int(os.getenv("POOL_SIZE", 4))) as executor:
                futures = {executor.submit(download, p): p for p in paths}
                for future in as_completed(futures):
                    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--frm", help="Date from in YYYY-MM-DD")
    parser.add_argument("--to", help="Date to in YYYY-MM-DD")
    parser.add_argument("--key", help="PEM key file path")
    parser.add_argument("--rpath", help="Remote base path")
    cmd_args = parser.parse_args()
    date_from = datetime.strptime(cmd_args.frm, '%Y-%m-%d').date()
    date_to = datetime.strptime(cmd_args.to, '%Y-%m-%d').date()
    key = cmd_args.key
    base_path = cmd_args.rpath

    dirs = []
    with open("desc.txt") as f:
        for line in f.readlines():
            if not line.startswith("#"):
                parts = line.split(":")
                dirs.append(DirectoryDesc(parts[0].strip(), parts[1].strip()))
    logger.debug(dirs)
    main(key, base_path, dirs=dirs, date_from=date_from, date_to=date_to)
