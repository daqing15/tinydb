from config import databases
import MySQLdb
import MySQLdb.cursors

class DatabaseFactory:
	connections = {}

	@staticmethod
	def instance(database = 'default'):
		MySQLdb.paramstyle = 'qmark'
		if database in DatabaseFactory.connections:
			return DatabaseFactory.connections[database]

		dbinfo = DatabaseFactory.get_properties(database)
		if dbinfo is None:
			return None
		db = None

		try:
			db = MySQLdb.connect(
				host = dbinfo['host'],
				user = dbinfo['user'],
				passwd = dbinfo['password'],
				db = dbinfo['dbname'],
				cursorclass = MySQLdb.cursors.DictCursor
			)
			db.autocommit(True)
			DatabaseFactory.connections[database] = db

		except:
			pass

		return db

	@staticmethod
	def close_connections(uno):
		for key in DatabaseFactory.connections:
			DatabaseFactory.connections[key].close()
			DatabaseFactory.connections = {}

	@staticmethod
	def get_property(database, p):
		dbinfo = DatabaseFactory.get_properties(database)
		if dbinfo is not None and p in dbinfo:
			return dbinfo[p]
		return None

	@staticmethod
	def get_properties(database):
		if database in databases:
			return databases[database]
		return None
