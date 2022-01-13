# -*- coding: utf-8 -*-

import json
import io


def get_name_url_file(url):
    split_one = url.split('/')
    for i in split_one:
        if '.jpg' in i or '.png' in i or '.mp4' in i or '.pdf' in i:
            split_two=i.split('?')
            return split_two[0]

all_data=[]
with io.open('temp222.json', encoding='utf-8') as file:
    data = json.load(file)
    for elem in data:
        for k,v in elem.items():
            get_data_sub=v[0].get('subattachments')
            if get_data_sub is not None:
                for t in get_data_sub.get('data'):
                    if t['media'].get('source') is not None:
                        name = get_name_url_file(t['media'].get('source'))
                        all_data.append({k: [{'url': t['media']['source'],
                                                              'name': name,
                                                              'height': t['media']['image']['height'],
                                                              'width': t['media']['image']['width']}]})
                    if 'static.' not  in t['media']['image']['src']:
                        name=get_name_url_file(t['media']['image']['src'])
                        all_data.append({k:[{'url':t['media']['image']['src'],
                                                             'name':name,
                                                             'height':t['media']['image']['height'],
                                                             'width':t['media']['image']['width']}]})
                    if '.pdf' in t['target']['url']:
                        name = t['title']
                        all_data.append({k: [{'url': t['target']['url'],'name':name}]})

            get_data_desc = v[0].get('description')
            if get_data_desc is not None:
                if '.pdf' in t['target']['url']:
                    name = t['title']
                    all_data.append({k: [{'url': t['target']['url'],'name':name}]})
                get_media=v[0].get('media')
                if get_media is not None:
                    name = get_name_url_file(v[0]['media']['image']['src'])
                    all_data.append({k: [{'url': v[0]['media']['image']['src'],
                                                          'name': name,
                                                          'height': v[0]['media']['image']['height'],
                                                          'width': v[0]['media']['image']['width'],
                                                          'description':v[0].get('description')}]})
            get_data_media = v[0].get('media')
            if get_data_media is not None:
                if get_data_media.get('source') is not None:
                    name=get_name_url_file(get_data_media.get('source'))
                    all_data.append({k: [{'url': v[0]['media']['source'],
                                                          'name': name,
                                                          'height': v[0]['media']['image']['height'],
                                                          'width': v[0]['media']['image']['width']}]})



with open('download_files.json', 'w', encoding='utf-8') as f:
    json.dump(all_data, f, ensure_ascii=False, indent=4)