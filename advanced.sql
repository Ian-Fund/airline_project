
/*List all reservations source and destination only for valid trips*/
 select reservation_number, to_, from_ from reservation_table inner join route_table on (reservation_table.route_id = route_table.route_id) where status = 'C';





/*List all reservations source and destination only for invalid trips*/
 select reservation_number, to_, from_ from reservation_table inner join route_table on (reservation_table.route_id = route_table.route_id) where status = 'I';




/*count by month from reservation table*/ 

select to_char(flight_date,'Mon') as mon,
       extract(year from flight_date) as yyyy,
       count("reservation_number") as "Passengers"
from reservation_table inner join flight_table on (reservation_table.flight_number = flight_table.flight_number)
group by 1,2;


/*Busy days*/ 
select * from busy_days;




/*List trips not purchased by anyone*/
select flight_number
from flight_table c 
where c.flight_number not in (select flight_number from reservation_table); 


/*Customers who have traveled to every city*/

select distinct customer_id, count(route_id)=19 from reservation_table group by customer_id;



/*List customers who  have two or more invalid trips. List those invalid trips by customer*/

select customer_id 
from reservation_table
where  (select count(customer_id) from reservation_table where status = 'I') >= 2
group by customer_id;


/*final program sql commands*/

select * from Final;
/*update Final set total_booked = (select num_booked from flight_table where flight_number = x) where flight_number = x;

update Final set total_business = (select bus_total from flight_table where flight_number = x) where flight_number = x;

update Final set total_economy = (select econ_total from flight_table where flight_number = x) where flight_number = x;

update Final set total_amount = (((select total_economy from Final where flight_number = x) * 100) + (select total_business from Final where flight_number = x) * 200)) where flight_number = x; */





