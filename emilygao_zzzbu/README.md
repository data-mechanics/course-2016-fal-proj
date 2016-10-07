##Project proposal 

We decide to combine the following data:

  1. Boston Public Schools:<https://data.cityofboston.gov/dataset/Boston-Public-Schools-School-Year-2012-2013-/e29s-ympv>
  
  2. Employee Earnings Report:<https://data.cityofboston.gov/Finance/Employee-Earnings-Report-2015/ah28-sywy>
  
  3. Entertainment License:<https://data.cityofboston.gov/Permitting/Entertainment-Licenses/qq8y-k3gp>
  
  4. Active Food Establishment License:<https://data.cityofboston.gov/Permitting/Active-Food-Establishment-Licenses/gb6y-34cq>
  
  5. Hospital Locations:<https://data.cityofboston.gov/Public-Health/Hospital-Locations/46f7-2snz>

We will use the combination datasets to answer the question of whether the people with higher earnings would like to live in the place with more public services or not. Hospital, public school, entertainment place and the active food establishment are all public service which can show whether that area is busy or not. Also, we will sort the data of earning report to see where are the people with higher earning live by recording the zipcode of these address. Moreover, the rest four dataset will show the common public building in each area by checking the zipcode of each place. 

In order to retrieve the data, we need to run five python file which are earning.py,entertainment.py, hospital.py, restaurant.py and school.py. These files can help us retrieve the data and store the information into the mongodb so that we can use them for the later analysis.


##Instruction For Running
#Authentication:
The `auth.json` file contains the following information

{
   "db_username": "alice_bob",
   "db_password": "alice_bob"
}
 
##Run the program:

Run earning.py,entertainment.py, hospital.py, restaurant.py and school.py. to retrieve.

#After that:

1.Run hospital_school_merge.py, it will transform the hospital and school date.(merge them).

2.Run hospital_school_aggregate.py, it will aggregate the data, and list the number of school and hospital in each zipcode.

3.Run hospital_school_select.py, it will create a new data that that only have school and hospital with zipcode: 02215 and it
print their information.

