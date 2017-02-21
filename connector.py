import json
import re

class connector:

    __pid       = None
    __thread_id = None
    __db        = None

    def __init__(self,pid,thread_id,db):
        self.__pid = pid
        self.__thread_id = thread_id
        self.__db = db

    def check_data(self,data):
        try:
            json_data = json.loads(data)
        except ValueError, e:
            return self.return_data(None,"can't decode received json data")

        # json fields check
        for field in 'app','host','operation','query','params':
            if field not in json_data:
                return self.return_data(None,"one of necessary fields has been missed")

        # operation check
        if re.search('^(fetch|execute)$',json_data['operation']) is None:
            return self.return_data(None,"Wrong operation has been received")

        try:
            if json_data['operation'] == 'fetch':
                return self.return_data(self.__db.fetch_data(json_data['query'],json_data['params']))
            elif json_data['operation'] == 'execute':
                return self.return_data(self.__db.execute(json_data['query'],json_data['params']))
        except Exception as e:
            return self.return_data(None,str(e))

    def return_data(self,data,fail_msg = None):
        return json.dumps({'result': 'success' if fail_msg is None else 'fail',
            'pid': self.__pid, 'thread_id': self.__thread_id, 'data': data, 'fail_msg': fail_msg})
