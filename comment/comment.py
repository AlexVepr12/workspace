import json
import os
import uuid
from urllib.parse import urlparse

import requests

from common.const import DELETED_USER_UUID
from common.pgsql import get_connection
from common.s3 import S3Client
from common.workplace import ExportData

SELECT_ALL_WORKPLACE_GROUP = """SELECT workplace_id FROM groups WHERE workplace_id is not Null"""
# SELECT_ALL_WORKPLACE_GROUP = """SELECT workplace_id FROM groups WHERE workplace_id is not Null and workplace_id = '2230263473892230'"""
# SELECT_ALL_WORKPLACE_USER = """SELECT workplace_id FROM users WHERE workplace_id is not Null and workplace_id = '100074014989596'"""
SELECT_ALL_WORKPLACE_USER = """SELECT workplace_id FROM users WHERE workplace_id is not Null"""
#
SELECT_USER_BY_WORKPLACE_ID = """SELECT id
                                FROM users t
                                WHERE workplace_id = %s
                                LIMIT 1"""

SELECT_POST_BY_WORKPLACE_ID = """select 1 from post where workplace_id = %s"""

INSERT_GROUP_POST = """insert into post(parent_id,workplace_id,text,created_at,updated_at,user_id,from_group,group_id)
values (%s,%s,%s,%s,%s,%s,%s,%s) on conflict do nothing returning id;"""

SELECT_WORKPLACE_GROUP = """SELECT id FROM groups WHERE workplace_id = %s limit 1"""

INSERT_FILE = """INSERT INTO file (name, owner_id, type, description, metadata) VALUES (%s,%s,%s,%s,%s) on conflict do nothing returning id;"""

APPEND_FILE_TO_POST = """INSERT INTO post_file (file_id,post_id)VALUES (%s,%s)"""


class ExportPosts(ExportData):
    def __init__(self, names, tokens):
        super().__init__(names, tokens)

        self.s3 = S3Client()
        self.conn = get_connection()

    def append(self, params, post) -> str:
        wp_parent_id = post.get('parent_id')
        parent_id = None
        if wp_parent_id is not None:
            url = self.BASE_URL + f'{wp_parent_id}/?fields=parent_id,id,created_time,formatting,from,icon,link,' \
                                  f'message,name,target,object_id,picture,place,poll,' \
                                  f'status_type,properties,story,to,type,updated_time,child_attachments,full_picture,is_hidden'
            r = requests.get(url, params=params)
            parent_id = self.append(params, r.json())

        values = (parent_id,)
        values = values + (post.get('id'),)
        values = values + (post.get('message'),)
        values = values + (post.get('created_time'),)
        values = values + (post.get('updated_time'),)

        with self.conn.cursor() as cursor:
            cursor.execute(SELECT_USER_BY_WORKPLACE_ID, (post.get('from').get('id'),))
            user_uuid = cursor.fetchone()
            if user_uuid is None:
                user_uuid = (DELETED_USER_UUID,)

            try:
                values = values + user_uuid
                wp_user_or_group_id = post.get('id').split('_')[0]
                from_group = False

                cursor.execute(SELECT_WORKPLACE_GROUP, (wp_user_or_group_id,))
                from_group_uuid = cursor.fetchone()
                if from_group_uuid is not None:
                    from_group = True

                values = values + (from_group,)
                values = values + (from_group_uuid,)

                cursor.execute(INSERT_GROUP_POST, values)
            except Exception as e:
                print(post, e)

            post_id = cursor.fetchone()
            if post_id is not None and parent_id is None:
                wp_post_id = post.get('id')
                url = self.BASE_URL + f'{wp_post_id}/attachments'
                r = requests.get(url, params=params)
                if r.json().get('error') is None:
                    data = r.json().get('data')
                    if len(data) != 0:
                        for attach in data:
                            get_data_sub = attach.get('subattachments')
                            if get_data_sub is not None:
                                for t in get_data_sub.get('data'):
                                    media = t.get('media')
                                    if media is not None:
                                        media_src = None
                                        metadata = None
                                        if t.get('type') in ('video', 'video_inline', 'animated_image_video'):
                                            type = 'video'
                                            media_src = media.get('source')

                                        if t.get('type') == 'file_upload':
                                            type = 'doc'
                                            media_src = t.get('url')

                                        if t.get('type') == 'photo':
                                            type = 'photo'
                                            metadata = json.dumps({'height': media.get('image').get('height'),
                                                                   'width': media.get('image').get('width')})
                                            media_src = media.get('image').get('src')

                                        if media_src is not None:
                                            name = self.get_load_attach(media_src)
                                            cursor.execute(INSERT_FILE, (name, user_uuid, type, None, metadata))
                                            file_id = cursor.fetchone()
                                            if file_id is not None:
                                                cursor.execute(APPEND_FILE_TO_POST, (file_id, post_id))
                                        else:
                                            print(t.get('type'), "SUB")

                        if attach.get('media') is not None:
                            description = attach.get('description')

                            media_src = None
                            metadata = None
                            if attach.get('type') in ('video', 'video_inline', 'animated_image_video'):
                                type = 'video'
                                media_src = attach.get('media').get('source')

                            if attach.get('type') == 'file_upload':
                                type = 'doc'
                                media_src = attach.get('url')

                            if attach.get('type') == 'photo':
                                type = 'photo'
                                metadata = json.dumps({'height': attach.get('media').get('image').get('height'),
                                                       'width': attach.get('media').get('image').get('width')})
                                media_src = attach.get('media').get('image').get('src')

                            if media_src is not None:
                                name = self.get_load_attach(media_src)
                                cursor.execute(INSERT_FILE, (name, user_uuid, type, description, metadata))
                                file_id = cursor.fetchone()
                                if file_id is not None:
                                    cursor.execute(APPEND_FILE_TO_POST, (file_id, post_id))
                            else:
                                print(attach.get('type'), 'MAIN')

            self.conn.commit()
            cursor.close()

        return post_id

    def insert_posts(self, params, posts):
        with self.conn.cursor() as cursor:
            for post in posts:
                cursor.execute(SELECT_POST_BY_WORKPLACE_ID, (post.get('id'),))
                exist = cursor.fetchone()
                if exist:
                    continue

                self.append(params, post)

            cursor.close()

    def process(self, i):
        with self.conn.cursor() as cursor:
            cursor.execute(SELECT_ALL_WORKPLACE_GROUP)
            groups = cursor.fetchall()
            self.pagination(i, groups)
            cursor.execute(SELECT_ALL_WORKPLACE_USER)
            users = cursor.fetchall()
            self.pagination(i, users)

    def pagination(self, i, objects):
        params = self.ACCESS.get(f'{i}')
        for obj in objects:
            print("wp_group_id:", obj[0])
            url = self.BASE_URL + f'{obj[0]}/feed?fields=parent_id,id,created_time,formatting,from,icon,link,' \
                                  f'message,name,object_id,picture,place,poll,' \
                                  f'status_type,properties,story,to,type,' \
                                  f'updated_time,child_attachments,full_picture,is_hidden'

            r = requests.get(url, params=params)
            self.insert_posts(params, r.json().get('data'))
            while r.json().get('paging') is not None:
                try:
                    r = requests.get(r.json().get('paging').get('next'), params=params)
                    data = r.json().get('data')
                    self.insert_posts(params, data)
                except requests.exceptions.MissingSchema:
                    print('страницы кончились')
                    break

    def get_load_attach(self, file_url):
        try:
            r = requests.get(file_url)

            ext = os.path.splitext(os.path.basename(urlparse(file_url).path))[1]

            avatar_name = f'workplace/attachments/' + str(uuid.uuid4()) + ext
            self.s3.load_data(r.content, avatar_name, "social-files", False)
        except Exception as e:
            print('Ошибка загрузки', e, file_url)
            avatar_name = None
        return avatar_name

# def get_post_comments(self, i):
#     comm_post = "SELECT * FROM post"
#     res_gr = ex(DB_CONFIG, comm_post, 'v')
#     params = ed.ACCESS.get(f'{i}')
#     for j in res_gr:
#         url = ed.BASE_URL + f'{j[-1]}/comments'
#         r = requests.get(url, params=params)
#         data = r.json().get('data')
#         if data is not None:
#             if len(data) != 0:
#                 for k in data:
#                     if len(k['message']) != 0:
#                         # print('++++++++++++++++++++++++++++++')
#                         # print(k['created_time'])
#                         # print(k['from']['id'])
#                         # print(k['message'])
#                         comm_user = "SELECT * FROM users where user_id_temp=%s" % "'" + k['from']['id'] + "'"
#                         comm_user_res = ex(DB_CONFIG, comm_user, 'v')
#                         if len(comm_user_res) != 0:
#                             text = k['message']
#                             print(text)
#                             post_id = j[0]
#                             user_id = comm_user_res[0][0]
#                             created = k['created_time']
#                             updated = k['created_time']
#                             comm_comment = "INSERT INTO comment (text,post_id,user_id,created_at,updated_at)" \
#                                            " VALUES (%s,%s,%s,%s,%s)" % ("'" + text + "'",
#                                                                          "'" + post_id + "'",
#                                                                          "'" + user_id + "'",
#                                                                          "'" + created + "'",
#                                                                          "'" + updated + "'")
#                             ex(DB_CONFIG, comm_comment)

# def get_post_reactions(self, i):
#     for i in range(len(names)):
#         comm_post = "SELECT * FROM post"
#         res_post = ex(DB_CONFIG, comm_post, 'v')
#         params = ed.ACCESS.get(f'{i}')
#         for j in res_post:
#             url = ed.BASE_URL + f'{j[-1]}/reactions'
#             r = requests.get(url, params=params)
#             data = r.json().get('data')
#             if data is not None:
#                 for u in data:
#                     comm_user = "SELECT * FROM users where user_id_temp=%s" % "'" + u['id'] + "'"
#                     comm_user_res = ex(DB_CONFIG, comm_user, 'v')
#                     if len(comm_user_res) != 0:
#                         id_user = comm_user_res[0][0]
#                         id_post = res_post[0][0]
#                         comm_like = "INSERT INTO post_like (post_id,user_id)" \
#                                     " VALUES (%s,%s)" % ("'" + id_post + "'",
#                                                          "'" + id_user + "'")
#                         ex(DB_CONFIG, comm_like)

# def get_post_seen(self):
#     all_data=[]
#     for i in range(len(names)):
#         params = ed.ACCESS.get(f'{i}')
#         for t in range(len(df_group_posts)):
#             gr_id=df_group_posts.iloc[t].id
#             url = ed.BASE_URL + f'{gr_id}/seen'
#             r = requests.get(url, params=params)
#             data = r.json().get('data')
#             if len(data)!=0:
#                 all_data.append({gr_id:data})
#     return all_data
