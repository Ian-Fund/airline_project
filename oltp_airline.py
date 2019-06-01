import sys
import psycopg2
import  _thread
import threading
import time
import os
import random
import datetime

#  Connect to DB.
#  Retunrs connection and cursor as tuple
def connect():
    conn = psycopg2.connect(

        database="team9",
        user="cosc3303",
        host="/tmp/",
        password="team9"
    )
    # conn = psycopg2.connect(
    #
    #     database="testdb",
    #     user="ian",
    #     host="localhost",
    #     password="password"
    # )
    cur = conn.cursor()
    return conn, cur

# Complete Transaction in each thread
def transaction(customer, seat, flight):
    date = str(datetime.datetime.today()).split()[0]
    db_connection = connect()
    conn = db_connection[0]
    cur = db_connection[1]
    print("Random Customer is: {} , {} , {}".format(customer,seat,flight))
    # cur.execute("begin;")
    cur.execute("select * from flight_table where flight_number={} for update".format(flight))
    # Erase after testing
    # cur.execute("select * from flight_table where flight_number=1")
    is_full = cur.fetchall()
    # conn.commit()
    # print("Table data is: {}".format(is_full[0][2]))
    if is_full[0][2] == 10:
        print("We full baby")
        # cur.execute("begin;")
        cur.execute("insert into reservation_table values(default, {},{},'{}',{},'N','I','{}') ".format(flight,customer,date
                                                                                                   ,flight,seat))
        conn.commit()
    else:
        print("Seats open")
#       Check if seat is open
#         cur.execute("begin;")
        cur.execute("select {} from flight_table where flight_number = {} ".format(seat,flight))
#         cur.execute("select one from flight_table where flight_number = 2; ")
        is_open = cur.fetchall()
        # print("The seat is: {}".format(is_open[0][0]))
        if is_open[0][0] == 1:
            print("Your seat is taken")
            # cur.execute("begin;")
            cur.execute("insert into reservation_table values(default, {},{},'{}',{},'N','I','{}') ".format(flight, customer,
                                                                                                    date, flight, seat))
            conn.commit()
        else:
            print("Your seat is open")
#           Biggest lock here

            if seat == 'one' or seat == 'two' or seat == 'three' or seat == 'four':
                # cur.execute("begin;")
                cur.execute("update flight_table set {} = 1 where flight_number = {} ".format(seat, flight))
                cur.execute(
                    "insert into reservation_table values(default, {},{},'{}',{},'N','C','{}') ".format(flight, customer
                                                                                                       , date, flight,
                                                                                                       seat))
                cur.execute("update flight_table set num_booked = (select num_booked from flight_table where flight_number =  {flight})+1 where flight_number = {flight} ".format(flight=flight))
                cur.execute(
                    "update flight_table set bus_booked = (select bus_booked from flight_table where flight_number =  {flight})+1 where flight_number = {flight} ".format(
                        flight=flight))
                conn.commit()
                # cur.execute("begin;")
                cur.execute("update customer_table set business_total = (select business_total from customer_table where "
                            "customer_id = {cust} for update) +200 where customer_id = {cust} ".format(cust=customer))
                conn.commit()
            else:
                # cur.execute("begin;")
                cur.execute("update flight_table set {} = 1 where flight_number = {} ".format(seat, flight))
                cur.execute("insert into reservation_table values(default, {},{},'{}',{},'N','C','{}') ".format(flight, customer
                                                                                                , date, flight,seat))

                cur.execute(
                    "update flight_table set num_booked = (select num_booked from flight_table where flight_number =  {flight})+1 where flight_number = {flight} ".format(
                        flight=flight))
                cur.execute(
                    "update flight_table set econ_booked = (select econ_booked from flight_table where flight_number =  {flight})+1 where flight_number = {flight} ".format(
                        flight=flight))
                conn.commit()
                # cur.execute("begin;")
                cur.execute("update customer_table set economy_total = (select economy_total from customer_table where "
                            "customer_id = {cust} for update) +100 where customer_id = {cust} ".format(cust = customer))
                conn.commit()

            conn.commit()

#     Goes through CSV to get customer IDs to make random tansactions.
#  Returns list of customer ids as strings
def get_customers():
    cwd = os.getcwd()
    customers = open(cwd + '/customers_copy.csv', 'r')
    customers_list = []
    for i in customers:
        customers_list.append(i.split(",")[0])
    return customers_list

# Goes through something to get list of flights
# Returns list of flight IDs as strings
def get_flights():
    cwd = os.getcwd()
    flights = open(cwd + '/flight_numbers.csv', 'r')
    flight_list = []
    for i in flights:
        flight_list.append(i.strip("\n").split(",")[0])
    return flight_list


#
# Empty Tables in DB before program runs
def empty_tables(conn, cur):
    cur.execute("delete from reservation_table")
    conn.commit()
    # Update users to set money to zero
    cur.execute("update customer_table set economy_total  = 0, business_total = 0 where customer_id >0")
    conn.commit()
    # Reset flights to empty
    cur.execute("update flight_table set one = 0,two = 0,three = 0,four = 0,five = 0,six = 0,seven = 0,eight = 0,\
                nine = 0,ten = 0, num_booked = 0, econ_booked = 0, bus_booked = 0 where flight_number  >0")
    conn.commit()

#     Reset profit tables once we make them



# Get paramaters
nthreads = int(sys.argv[1].split('=')[1])
secs = int(sys.argv[2].split('=')[1])

# Get connection and cursor for DB
db_connection = connect()
conn = db_connection[0]
cur = db_connection[1]

# Empty Tables before starting
empty_tables(conn,cur)

# Get lists to randomly generate transactions
customer_list = get_customers()
fare_list = ['one','two','three','four','five','six','seven','eight','nine','ten']
flight_list = get_flights()




try:
    # This is where threading happens
    thread_list = []
    while True:
        for thread in range(0,nthreads):
            print("Creating thread {}".format(thread))
            # MAKE SQL STATEMENT HERE
            # sql = ("Random customer is: {}, {}, {}".format(customer_list[random.randint(0,49)],fare_list[random.randint(0,9)],flight_list[random.randint(0,18)]))
            # sql = ("")
            # print("Random Customer is {}".format(sql))
            # _thread.start_new_thread(transaction,(customer_list[random.randint(0,49)],fare_list[random.randint(0,9)],flight_list[random.randint(0,18)]))
            # transaction(customer_list[random.randint(0,49)],fare_list[random.randint(0,9)],flight_list[random.randint(0,18)])

        # Above this line works
            args = []
            args.append(customer_list[random.randint(0,49)])
            args.append(fare_list[random.randint(0,9)])
            args.append(flight_list[random.randint(0,18)])
            t = threading.Thread(target=transaction,args=args)
                # transaction(customer_list[random.randint(0,49)],fare_list[random.randint(0,9)],flight_list[random.randint(0,18)])
            thread_list.append(t)
            t.start()


        time.sleep(secs)

except KeyboardInterrupt:
    for x in thread_list:
        x.join()

    for i in range (1,20):
        cur.execute("update Final set total_booked = (select num_booked from flight_table where flight_number = {}) where flight_number = {}".format(i,i))
        cur.execute("update Final set total_business = (select bus_booked from flight_table where flight_number = {}) where flight_number = {}".format(i,i))
        cur.execute("update Final set total_economy = (select econ_booked from flight_table where flight_number = {}) where flight_number = {}".format(i,i))
        cur.execute("update Final set total_amount = (((select total_economy from Final where flight_number = {}) * 100) + (select total_business from Final where flight_number = {}) * 200) where flight_number = {} ".format(i,i,i))
        conn.commit()