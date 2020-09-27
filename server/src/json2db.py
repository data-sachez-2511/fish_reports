import json
import datetime
import re
import os
import sqlite3


data = json.loads(open('FORUM.json', 'r').read())
total = len(list(data['date']))
results = []

for entry in zip(list(data['date'].values()), list(data['is_report'].values()), list(data['main_place'].values()), list(data['place'].values()), list(data['text'].values())):
    if len(entry[4]) > 5000:
        continue
    date = entry[0]
    if isinstance(date, str):
        if re.fullmatch('\d\d.\d\d.\d\d\d\d', date.strip()):
            date = datetime.datetime(int(date.split('.')[2]), int(date.split('.')[1]), int(date.split('.')[0])).timestamp()
        else:
            continue
    else:
        date /= 1000
    text = entry[4]
    emoticons = 0
    while True:
        match = re.search('(<\s*(\/)?\s*[^\s<>"]+(?(1)|( [^\s<>"]+(="[^"]*")?)*)\s*>)|((\:\s?\w+\s?\:|\<[\/\\]?3|[\(\)\\\Dd|\*\$][\-\^]?[\:\;\=]|[\:\;\=B8][\-\^]?[3DdOoPp\@\$\*\\\)\(\/\|])(?=\s|[\!\.\?]|$))', text)
        if match is not None:
            text = text.replace(match.group(0), '', 1)
            if match.group(5):
                emoticons += 1
        else:
            while True:
                match = re.search(' {2,}', text)
                if match is not None:
                    text = text.replace(match.group(0), ' ', 1)
                else:
                    break
            break
    if text.strip() == '':
        continue
    results.append((date, entry[1], entry[2], entry[3], text, emoticons))

if os.path.exists('reports.db'):
    os.remove('reports.db')
conn = sqlite3.connect('reports.db')
cursor = conn.cursor()
cursor.execute('CREATE TABLE reports (id INTEGER NOT NULL UNIQUE, date INTEGER NOT NULL, is_report BOOLEAN NOT NULL, main_place TEXT NOT NULL, place TEXT NOT NULL, text TEXT NOT NULL, emoticons INTEGER NOT NULL, PRIMARY KEY (id))')
cursor.executemany('INSERT INTO reports (date, is_report, main_place, place, text, emoticons) VALUES (?, ?, ?, ?, ?, ?)', results)
conn.commit()
conn.close()
