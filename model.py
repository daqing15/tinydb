from factory import DatabaseFactory
import MySQLdb

class Model:
	def __init__(self, tablename, idname = 'id', connection = 'default'):
		dbname = DatabaseFactory.get_property(connection, 'dbname')
		self.tablename = dbname + '.' + tablename
		self.idname = idname
		self.db = DatabaseFactory.instance(connection)

	def get_all(self, start = None, count = None):
		sql = 'select * from %s' % self
		if start is not None:
			sql += ' limit %s' % start
		if count is not None:
			sql += ', %s' % count
		return self.do_query(sql)

	def get_by(self, key, value):
		sql = 'select * from %s' % self
		sql += ' where %s = %s' % (key, '%s')
		return self.do_query(sql, str(value))

	def get(self, id):
		result = self.get_by(self.idname, id)
		if result is None:
			return None
		return result[0]

	def get_unique(self, unique, value):
		result = self.get_by(unique, value)
		if result is None:
			return None
		return result[0]

	def insert(self, *fields):
		sql = 'insert into %s values(' % self
		temp = []
		for field in fields:
			temp.append('%s')
		sql += ', '.join(temp) + ')'
		return self.do_non_query(sql, *fields)

	def delete_by(self, key, value):
		sql = 'delete from %s where %s = %s' % (self, key, '%s')
		return self.do_non_query(sql, value)

	def delete(self, id):
		return self.delete_by(self.idname, id) > 0
		pass

	def delete_all(self):
		return self.do_non_query('delete from %s' % self)

	def update_by(self, key, value, update_key, update_value):
		sql = 'update %s set %s = %s where %s = %s' % (self, update_key, '%s', key, '%s')
		return self.do_non_query(sql, update_value, value)

	def update(self, id, key, value):
		return self.update_by(self.idname, id, key, value) > 0

	def exists_by(self, key, value):
		sql = 'select count(*) from %s where %s = %s' % (self, key, '%s')
		return self.do_scalar(sql, value) > 0

	def exists(self, id):
		return self.exists_by(self.idname, id)

	def last_insert_id(self):
		return self.db.insert_id()

	def error(self):
		pass

	def version(self):
		return self.do_scalar('select version()')

	def begin(self):
		self.db.autocommit = False

	def commit(self):
		self.db.commit()
		self.db.autocommit = True

	def now(self):
		return self.do_scalar('select now()')

	def today(self):
		return self.do_scalar('select current_date')

	def count(self, what = None, distinct = False):
		count = '*'
		if what is not None:
			count = what
			if distinct:
				count = 'distinct %s' % count
		sql = 'select count(%s) from %s' % (count, self)
		return self.do_scalar(sql)

	def max(self, expr):
		return self.do_scalar('select max(%s) from %s' % (expr, self))

	def min(self, expr):
		return self.do_scalar('select min(%s) from %s' % (expr, self))

	def sum(self, expr):
		return self.do_scalar('select sum(%s) from %s' % (expr, self))

	def avg(self, expr):
		return self.do_scalar('select avg(%s) from %s' % (expr, self))

	def distinct(self, *columns):
		what = ', '.join(columns)
		sql = 'select distinct %s from %s' % (what, self)
		return self.do_query(sql)

	def get_like(self, _dict, empty_gets_all = True):
		if _dict is None or len(_dict) is 0:
			return self.get_all() if empty_gets_all else None
		sql = 'select * from %s where ' % self
		criteria = []
		params = []
		for key in _dict:
			criteria.append('%s like %s' % (key, '%s'))
			params.append('%' + _dict[key] + '%')
		sql += ' or '.join(criteria)
		return self.do_query(sql, *params)

	def do_query(self, sql, *params):
		cursor = self.db.cursor()
		cursor.execute(sql, params)
		result = cursor.fetchall()
		cursor.close()
		if len(result) is 0:
			return None
		return result

	def do_non_query(self, sql, *params):
		cursor = self.db.cursor()
		cursor.execute(sql, params)
		rowcount = cursor.rowcount
		cursor.close()
		return rowcount

	def do_scalar(self, sql, *params):
		cursor = self.db.cursor()
		cursor.execute(sql, params)
		result = cursor.fetchone()
		cursor.close()
		if result is None or len(result) is 0:
			return None
		for key in result:
			return result[key] #return just the first value

	def __str__(self):
		return self.tablename
