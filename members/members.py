# utf-8
import secrets

import pandas as pd
import requests

from common.const import ADMIN_USER_UUID, DELETED_USER_UUID
from common.pgsql import get_connection
from common.s3 import S3Client
from common.workplace import ExportData

INSERT_USER = """insert into users (first_name,last_name,email,password,created_at,title,workplace_id,phone,address,department,about)
values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict do nothing returning id;"""

INSERT_ADMIN_USER = """insert into users (id, first_name,last_name,email,password)
values (%s, 'Admin', '@', 'admin@efko.ru', '$argon2id$v=19$m=65536,t=3,p=2$gtZ2JzP/l7dSTcKlIk0v4Q$nHISyOOmB+3/h0MpXzRr3zdMqbMP0BiOY427srK4Z94') on conflict do nothing;"""

INSERT_DELETED_USER = """insert into users (id, first_name,last_name,email,password)
values (%s, 'deleted', '@', 'deleted@efko.ru', '$argon2id$v=19$m=65536,t=3,p=2$gtZ2JzP/l7dSTcKlIk0v4Q$nHISyOOmB+3/h0MpXzRr3zdMqbMP0BiOY427srK4Z94') on conflict do nothing;"""

INSERT_FILE = """INSERT INTO file (name, owner_id, type) VALUES (%s,%s,%s) returning id;"""

APPEND_AVATAR_TO_USER = """UPDATE users SET avatar = %s where id = %s;"""


class ExportMembers(ExportData):
    def __init__(self, names, tokens):
        super().__init__(names, tokens)

        self.s3 = S3Client()
        self.conn = get_connection()

        with self.conn.cursor() as cursor:
            cursor.execute(INSERT_ADMIN_USER, (ADMIN_USER_UUID,))
            cursor.execute(INSERT_DELETED_USER, (DELETED_USER_UUID,))
            self.conn.commit()

    def get_load_avatar(self, id, avatar_url):
        try:
            r = requests.get(avatar_url)
            avatar_name = f'workplace/avatars/{id}.jpg'
            self.s3.load_data(r.content, avatar_name, "social-files", False)
        except Exception as e:
            print('Ошибка загрузки', e)
            avatar_name = None
        return avatar_name

    def get_member(self, id, i):
        params = self.ACCESS.get(f'{i}')
        url = self.BASE_URL + f'{id}?fields=first_name,' \
                              f'last_name,email,title,' \
                              f'department,account_invite_time,' \
                              f'primary_address,primary_phone,about'

        r = requests.get(url, params=params)
        first_name = r.json().get('first_name')
        last_name = r.json().get('last_name')
        email = r.json().get('email')
        title = r.json().get('title')
        department = r.json().get('department')
        account_invite_time = r.json().get('account_invite_time')
        primary_address = r.json().get('primary_address')
        primary_phone = r.json().get('primary_phone')
        about = r.json().get('about')

        # получение ссылки на аватар
        url = self.BASE_URL + f'{id}/picture?type=large&width=720&height=720'
        rs = requests.get(url, params=params)

        avatar_url = None
        if rs.headers['content-type'] == "image/jpeg":
            avatar_url = rs.url

        print(r.json(), avatar_url)

        return first_name, last_name, email, title, department, account_invite_time, primary_address, avatar_url, primary_phone, about

    def insert_member(self, df, i):
        for row in range(len(df)):
            id = df.iloc[row, 1]
            (first_name, last_name, email,
             title, department, account_invite_time,
             primary_address, avatar_url, primary_phone, about) = self.get_member(id, i)

            if email is None:
                continue

            with self.conn.cursor() as cursor:
                cursor.execute(INSERT_USER, (first_name,
                                             last_name,
                                             email,
                                             secrets.token_urlsafe(32),
                                             account_invite_time,
                                             title,
                                             id, primary_phone, primary_address, department, about))
                user_uuid = cursor.fetchone()
                if user_uuid is not None and avatar_url is not None:
                    avatar_name = self.get_load_avatar(id, avatar_url)
                    if avatar_name is not None:
                        cursor.execute(INSERT_FILE, (avatar_name, user_uuid, "photo"))
                        file_id = cursor.fetchone()
                        if file_id is not None:
                            cursor.execute(APPEND_AVATAR_TO_USER, (file_id, user_uuid))

            self.conn.commit()

    def get_members(self, i):
        params = self.ACCESS.get(f'{i}')
        url = self.BASE_URL + f'community/members'
        r = requests.get(url, params=params)
        print('url', r.url)
        data = r.json().get('data')
        df = pd.DataFrame.from_dict(data)
        print('получено пользователей', len(df))
        self.insert_member(df, i)
        while r.json().get('paging') is not None:
            try:
                r = requests.get(r.json().get('paging').get('next'), params=params)
                print('url', r.url)
                data = r.json().get('data')
                df = pd.DataFrame.from_dict(data)
                print('получено пользователей', len(df))
                self.insert_member(df, i)
            except requests.exceptions.MissingSchema:
                print('страницы кончились')
                break
