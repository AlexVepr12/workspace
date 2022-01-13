# utf-8
import pandas as pd
import requests

from common.const import ADMIN_USER_UUID
from common.pgsql import get_connection
from common.s3 import S3Client
from common.workplace import ExportData

INSERT_COVER_FILE = """INSERT INTO file (name, owner_id, type) VALUES (%s,%s,%s) on conflict do nothing returning id;"""

INSERT_GROUP = """insert into groups(description,name,workplace_id,security_level,owner_id,avatar)
values (%s,%s,%s,%s,%s,%s) on conflict do nothing;"""

SELECT_USER_BY_WORKPLACE_ID = """SELECT id
                                FROM users t
                                WHERE workplace_id = %s
                                LIMIT 1"""

INSERT_GROUP_MEMBER = """INSERT INTO group_member (group_id,user_id,administrator) 
values (%s,%s,%s) on conflict do nothing"""

SELECT_WORKPLACE_GROUP = """SELECT * FROM groups WHERE workplace_id = %s"""


class ExportGroups(ExportData):
    def __init__(self, names, tokens):
        super().__init__(names, tokens)

        self.s3 = S3Client()
        self.conn = get_connection()

    def get_load_cover(self, id, cover_url):
        try:
            r = requests.get(cover_url)
            cover_name = f'workplace/group_cover/{id}.jpg'
            print(cover_name)
            self.s3.load_data(r.content, cover_name, "social-files", False)
        except Exception as e:
            print('Ошибка загрузки', e)
            cover_name = None
        return cover_name

    def insert_groups(self, df, i):
        for row in range(len(df)):
            group_id = df.iloc[row, 2]
            params = self.ACCESS.get(f'{i}')
            url = self.BASE_URL + f'{group_id}?fields=id,owner,' \
                                  f'description,is_community,' \
                                  f'name,privacy,cover'
            r = requests.get(url, params=params)

            if r.json().get('is_community'):
                continue

            values = ()
            owner_wp_id = r.json().get('owner')
            values = values + (r.json().get('description'),)
            values = values + (r.json().get('name'),)
            values = values + (r.json().get('id'),)

            if r.json().get('privacy') == "OPEN":
                values = values + ("public",)
            if r.json().get('privacy') == "CLOSED":
                values = values + ("private",)
            if r.json().get('privacy') == "SECRET":
                values = values + ("secret",)

            with self.conn.cursor() as cursor:
                if owner_wp_id is None:
                    values = values + (ADMIN_USER_UUID,)
                else:
                    cursor.execute(SELECT_USER_BY_WORKPLACE_ID, (owner_wp_id.get('id'),))
                    values = values + cursor.fetchone()

                file_id = (None,)
                cover = r.json().get('cover')
                if cover is not None:
                    cover_name = self.get_load_cover(group_id, cover.get('source'))
                    if cover_name is not None:
                        cursor.execute(INSERT_COVER_FILE, (cover_name, ADMIN_USER_UUID, "photo"))
                        file_id_tmp = cursor.fetchone()
                        if file_id_tmp is not None:
                            file_id = file_id_tmp

                values = values + file_id

                cursor.execute(INSERT_GROUP, values)
                cursor.execute(SELECT_WORKPLACE_GROUP, (group_id,))
                group_uuid = cursor.fetchone()
                if group_uuid is not None:
                    self.group_members(i, group_id, group_uuid[0])

                self.conn.commit()

    def insert_group_members(self, members, group_id):
        for member in members:
            with self.conn.cursor() as cursor:
                cursor.execute(SELECT_USER_BY_WORKPLACE_ID, (member.get('id'),))
                user_id = cursor.fetchone()
                print(member, group_id, user_id)
                cursor.execute(INSERT_GROUP_MEMBER, (group_id, user_id[0], member.get('administrator'),))

                self.conn.commit()

    def group_members(self, i, group_workplace_id, group_social_id):
        params = self.ACCESS.get(f'{i}')
        url = self.BASE_URL + group_workplace_id + '/members'
        r = requests.get(url, params=params)
        data = r.json().get('data')
        self.insert_group_members(data, group_social_id)
        while r.json().get('paging') is not None:
            try:
                r = requests.get(r.json().get('paging').get('next'), params=params)
                print('url', r.url)
                data = r.json().get('data')
                self.insert_group_members(data, group_social_id)
            except requests.exceptions.MissingSchema:
                print('страницы кончились')
                break

    def get_community_groups(self, i):
        """
        :param i:
        :return:

        1. запрос инфомации по полям группы
        2. инсерт в таблицу в аватарами аватар
        3. запрос информации

        """
        params = self.ACCESS.get(f'{i}')
        url = self.BASE_URL + 'community/groups'
        r = requests.get(url, params=params)
        data = r.json().get('data')
        df = pd.DataFrame.from_dict(data)
        self.insert_groups(df, i)
        while r.json().get('paging') is not None:
            try:
                r = requests.get(r.json().get('paging').get('next'), params=params)
                data = r.json().get('data')
                df = pd.DataFrame.from_dict(data)
                self.insert_groups(df, i)
            except requests.exceptions.MissingSchema:
                print('страницы кончились')
                break
