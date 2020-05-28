from pymongo import MongoClient
import psycopg2
import datetime

calculate_time_diff = lambda toTime,fromTime : toTime - fromTime
convert_hrs = lambda deltaTime : (deltaTime.seconds)/3600
convert_days = lambda deltaTime : (deltaTime.seconds)/(3600*24)

def connect_mongoDb():
    myclient = MongoClient("mongodb://mongo:ubuntu2020@machines-shard-00-00-wztpt.mongodb.net:27017,machines-shard-00-01-wztpt.mongodb.net:27017,machines-shard-00-02-wztpt.mongodb.net:27017/test?ssl=true&replicaSet=machines-shard-0&authSource=admin&retryWrites=true&w=majority")
    mydb = myclient["mqtt_machines"]
    mycol = mydb["machine_data"]
    #print("connected to mongoDB")
    return mycol

def connect_sql():
    connection = psycopg2.connect(user = "ubuntu",
                                      password = "ubuntu2020",
                                      host = "mqtt-test-deploy.c3sluijwelvv.us-east-2.rds.amazonaws.com",
                                      port = "5432",
                                      database = "mqtt")
    #cursor = connection.cursor()
    #print("connected to SQL")
    return connection

def fetch_sql_time(conn):
    cursor = conn.cursor()
    postgres_select_query = "SELECT NOW();"
    cursor.execute(postgres_select_query)
    records = cursor.fetchall()
    conn.commit()
    return records[0][0]

#fetch all machines data for particular parameter
def fetch_param_all(mycol,parameter):
    out = []
    i = 0
    records = mycol.find({},{ "_id": 0,"machine_name": 1,parameter: 1})
    for x in records:
        out.insert(i,x[parameter])
        i = i+1
    return out


    
class Machine:
    count = 0
    def __init__(self,machine_name):
        self.machine_name = machine_name
        self.topic_name = "/"+machine_name
        self.sql_table = machine_name
        self.sql_table_daily = machine_name+"_daily"
        
        Machine.count +=1

    def fetch_all_data_btw(self,conn,fromTime,toTime):
        cursor = conn.cursor()
        postgres_select_query = "SELECT * FROM "+self.sql_table+" WHERE time_stamp >= '"+fromTime+"'AND time_stamp <= '"+toTime+"';"
        cursor.execute(postgres_select_query)
        records = cursor.fetchall()
        conn.commit()
        return records

    def fetch_all_data_param_btw(self,conn,parameter,fromTime,toTime):
        cursor = conn.cursor()
        postgres_select_query = "SELECT "+parameter+" FROM "+self.sql_table+" WHERE time_stamp >= '"+fromTime+"'AND time_stamp <= '"+toTime+"';"
        cursor.execute(postgres_select_query)
        records = cursor.fetchall()
        conn.commit()
        return records

    def fetch_power_data_btw(self,conn,fromTime,toTime):
        cursor = conn.cursor()
        postgres_select_query = "SELECT power,time_stamp FROM "+self.sql_table+" WHERE time_stamp >= '"+fromTime+"'AND time_stamp <= '"+toTime+"';"
        cursor.execute(postgres_select_query)
        records = cursor.fetchall()
        conn.commit()
        return records

    #fetch all power_out data with time stamp
    def fetch_power_out_data(self,conn,fromTime,toTime):
        cursor = conn.cursor()
        postgres_select_query = "SELECT power_out,time_stamp FROM "+self.sql_table+" WHERE time_stamp >= '"+fromTime+"'AND time_stamp <= '"+toTime+"';"
        cursor.execute(postgres_select_query)
        records = cursor.fetchall()
        conn.commit()
        return records


    #fetch all power_out data until now
    def fetch_power_out_data_all(self,conn):
        cursor = conn.cursor()
        postgres_select_query = "SELECT power_out,time_stamp FROM "+self.sql_table+";"
        cursor.execute(postgres_select_query)
        records = cursor.fetchall()
        conn.commit()
        return records

    # fetch all non zero power_out data for that day
    def non_zero_power_all(self,conn):
        cursor = conn.cursor()
        postgres_select_query = "SELECT power_out,time_stamp FROM "+self.sql_table+" WHERE power_out > 0;"
        cursor.execute(postgres_select_query)
        records = cursor.fetchall()
        conn.commit()
        return records

    # fetch all power_out data for that day
    def fetch_power_data_day(self,conn):
        cursor = conn.cursor()
        postgres_select_query = "SELECT power_out,time_stamp FROM "+self.sql_table+" WHERE EXTRACT(day from time_stamp) = EXTRACT(day from (select now()));"
        cursor.execute(postgres_select_query)
        records = cursor.fetchall()
        conn.commit()
        return records

    # fetch all non zero for that day
    def fetch_non_zero_power_data_day(self,conn):
        cursor = conn.cursor()
        postgres_select_query = "SELECT power_out,time_stamp FROM "+self.sql_table+" WHERE EXTRACT(day from time_stamp) = EXTRACT(day from (select now())) AND power_out > 0;"
        cursor.execute(postgres_select_query)
        records = cursor.fetchall()
        conn.commit()
        return records

    # power_out data after time stamp
    def power_data_after_time(self,conn,tstamp):
        cursor = conn.cursor()
        postgres_select_query = "SELECT power_out,time_stamp FROM "+self.sql_table+" WHERE time_stamp >= '"+tstamp+"';"
        cursor.execute(postgres_select_query)
        records = cursor.fetchall()
        conn.commit()
        return records

    #non zero power out after time
    def non_zero_power_after_time(self,conn,tstamp):
        cursor = conn.cursor()
        postgres_select_query = "SELECT power_out,time_stamp FROM "+self.sql_table+" WHERE time_stamp >= '"+tstamp+"' AND power_out > 0;"
        cursor.execute(postgres_select_query)
        records = cursor.fetchall()
        conn.commit()
        return records
    
    #non zero power out after time on the day
    def non_zero_power_after_time_day(self,conn,tstamp):
        cursor = conn.cursor()
        postgres_select_query = "SELECT power_out,time_stamp FROM "+self.sql_table+" WHERE EXTRACT(day from time_stamp) = EXTRACT(day from (select now())) AND time_stamp >= '"+tstamp+"' AND power_out > 0;"
        cursor.execute(postgres_select_query)
        records = cursor.fetchall()
        conn.commit()
        return records


    #fetch installation time    
    def installation_time(self,conn):
        cursor = conn.cursor()
        postgres_select_query = "SELECT time_stamp FROM "+self.sql_table+" ORDER BY time_stamp ASC LIMIT 1"
        cursor.execute(postgres_select_query)
        records = cursor.fetchall()
        conn.commit()
        return records[0][0]

    #calculate_uptime_days
    def ins_uptime(self,conn):
        days = 0
        cursor = conn.cursor()
        current_time = self.fetch_sql_time(conn)
        postgres_select_query = "SELECT time_stamp FROM "+self.sql_table+" ORDER BY time_stamp ASC LIMIT 1"
        cursor.execute(postgres_select_query)
        records = cursor.fetchall()
        conn.commit()
        diff = calculate_time_diff(current_time,records[0][0])
        return diff


    def fetch_machine_data_mdb(self,mycol,parameter):
        out =[]
        i = 0
        myquery = { "machine_name": self.machine_name }
        records = mycol.find(myquery,{ "_id": 0,parameter: 1})
        for x in records:
            out.insert(i,x[parameter])
            i = i+1
            
        return out[0]
    
    
    #update machine data for particular parameter
    def update_machine_data_mdb(self,mycol,parameter,value):
        myquery = { "machine_name": self.machine_name }
        newvalues = { "$set": { parameter: value } }
        mycol.update_one(myquery, newvalues)
        




if __name__ == '__main__':
    print('this is a class file')

else:
    print('class imported')

      





    
        
        


    


