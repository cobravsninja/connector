import datetime

class DB:
	__connection	= None
	__cur		= None

        # credentials
        __username      = None
        __password      = None
        __host          = None
        __instance      = None

        def __init__(self,username,password,host,instance):
            self.__username = username
            self.__password = password
            self.__host = host
            self.__instance = instance
            self.connect()

        def connect(self):
            import cx_Oracle
	    self.__connection = cx_Oracle.connect('{username}/{password}@{host}/{instance}'.format(username=self.__username,
            password=self.__password,host=self.__host,instance=self.__instance))
            self.__connection.autocommit = True

        def fetch_data(self,select,params):
	    data = self.execute(select,params)
            if isinstance(data,basestring) is False:
	        return self.__rows_to_dict_list(data)

	def execute(self,select,params,out = None):
            params = {} if params is None else params
            try:
		self.__cur = self.__connection.cursor()
                if out is None:
    		    self.__cur.prepare(select)
    		    return self.__cur.execute(None,params)
                else:
                    params['out'] = self.__cur.var(cx_Oracle.NUMBER)
    		    self.__cur.prepare(select)
    		    self.__cur.execute(None,params)
                    return int(params['out'].getvalue())
            except Exception as e:
                raise Exception('{}'.format(e))

	def close_connection(self):
		self.__connection.close()

	def __rows_to_dict_list(self,cursor):
		columns = [i[0] for i in cursor.description]
                data = [dict(zip(columns, row)) for row in cursor]
                for row in data:
                    for key in row:
                        if isinstance(row[key],datetime.datetime):
                            row[key] = str(row[key])
                return data
