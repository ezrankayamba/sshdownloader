from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import paramiko
import argparse


key = r"C:\NKAYAMBA\Keys\Nezatech\LightsailDefaultKey-eu-west-2.pem"


class DirectoryDesc:
    def __init__(self, src, dst) -> None:
        self.src = src
        self.dst = dst

    def __str__(self) -> str:
        return f'{self.src} => {self.dst}'


def connect():
    host = "3.9.198.111"
    special_account = "ubuntu"
    pkey = paramiko.RSAKey.from_private_key_file(key)
    client = paramiko.SSHClient()
    policy = paramiko.AutoAddPolicy()
    client.set_missing_host_key_policy(policy)
    client.connect(host, username=special_account, pkey=pkey)
    return client


def main(base_path: str = '/home/ubuntu/apps/twiga_sales/backend_rest/media', dirs: list[DirectoryDesc] = None, date_from=None, date_to=None):
    client = connect()
    print(client)

    with client.open_sftp() as sftp:
        print('Connected ...')

        def download_file(d: DirectoryDesc, p: paramiko.SFTPAttributes):
            m_time = datetime.fromtimestamp(p.st_mtime).date()
            if m_time >= date_from and m_time <= date_to:
                r_path = f'{base_path}/{d.src}/{p.filename}'
                l_path = f'download\{d.dst}\{p.filename}'
                with sftp.open(r_path, "r") as f:
                    sftp.get(f, l_path)
                print(f'Downloaded: {d.src} => {p.filename}/{m_time}')
            else:
                print(f'Skipped: {d.src} => {p.filename}/{m_time}')

        if dirs:
            for d in dirs:
                remote_dirs = sftp.listdir_iter(path=f'{base_path}/{d.src}')
                with ThreadPoolExecutor(max_workers=10) as executor:
                    future = {executor.submit(
                        download_file, d, p): p for p in remote_dirs}
                    for future in as_completed(future):
                        data = future.result()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--frm", help="Date from in YYYY-MM-DD")
    parser.add_argument("--to", help="Date to in YYYY-MM-DD")
    cmd_args = parser.parse_args()
    date_from = datetime.strptime(cmd_args.frm, '%Y-%m-%d').date()
    date_to = datetime.strptime(cmd_args.to, '%Y-%m-%d').date()

    dirs = [
        DirectoryDesc('users/profile_photos', 'users/profile_photos'),
        # DirectoryDesc('batches', 'batches'),
        # DirectoryDesc('docs', 'docs'),
        # DirectoryDesc('docs-invoice', 'docs-invoice'),
    ]
    main(dirs=dirs, date_from=date_from, date_to=date_to)
