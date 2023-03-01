import pandas as pd
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()

class BaseETL():

    def conn(self, db_name):
        
        engine = create_engine("mysql+mysqldb://SC:cnuh12345!@127.0.0.1:3306/{}".format(db_name), encoding = 'utf-8')
        
        return engine

    def df_from_sql(self, db_name, sql):
        
        print("from {}".format(db_name))
        #print(sql)
        
        engine = self.conn(db_name)
        
        return pd.read_sql(sql, engine)

    def insert(self, df, db_name, tb_name, if_exists = "replace"):
        
        engine = self.conn(db_name)

        df.to_sql(con = engine, name = tb_name, if_exists = if_exists, index = False)
        
        print("to {}".format(db_name))


if __name__ == "__main__":
    obj = BaseETL()