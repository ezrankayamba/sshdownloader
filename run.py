from datetime import datetime
import paramiko
import argparse
import decorators
import sqlite3


class DirectoryDesc:
    def __init__(self, src, dst) -> None:
        self.src = src
        self.dst = dst

    def __repr__(self) -> str:
        return f'{self.src} => {self.dst}'


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
    client = connect(key)
    print(client)

    con = sqlite3.connect("download.db")
    cur = con.cursor()

    def record_download(l_path: str, m_date):
        sql = f"INSERT INTO downloads (path, mdate) VALUES ('{l_path}', '{m_date}')"
        cur.execute(
            sql).fetchone()
        con.commit()

    def check_exists(l_path: str) -> bool:
        sql = f"SELECT * FROM downloads WHERE path = '{l_path}'"
        res = cur.execute(
            sql).fetchone()
        return True if res else False

    @decorators.timer()
    def download_file(sftp, r_path, l_path, m_time):
        if not check_exists(l_path):
            sftp.get(r_path, l_path)
            record_download(l_path, m_time)
            print(f'Downloaded: {r_path} => {l_path}/{m_time}')

    if dirs:
        for d in dirs:
            paths = []
            with client.open_sftp() as sftp:
                print('Connected ...')
                remote_dirs = sftp.listdir_attr(path=f'{base_path}/{d.src}')
                for p in remote_dirs:
                    m_time = datetime.fromtimestamp(p.st_mtime).date()
                    if m_time >= date_from and m_time <= date_to:
                        r_path = f'{base_path}/{d.src}/{p.filename}'
                        l_path = f'{d.dst}\{p.filename}'
                        paths.append((r_path, l_path, m_time))

            paths.sort(key=lambda p: p[2], reverse=True)
            with client.open_sftp() as sftp:
                for p in paths:
                    download_file(sftp, p[0], p[1], p[2])

    client.close()
    print('Done')


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
    print(dirs)
    main(key, base_path, dirs=dirs, date_from=date_from, date_to=date_to)
