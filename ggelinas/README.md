#Boston Crime Rates in Relation to Hospitals, Police Districts and Property

##Introduction
In this project, using data from the city of Boston we seek to understand crime rates in relation to hospitals, police 
districts, and property. We wanted to determine:
<ol>
    <li>If crime were more likely in areas of lower property values
    <li>If hospitals are optimally located around crime areas
</ol>

The data sets being used for analysis are acquired publicly from the city of Boston data portal. Furthermore, the techniques
being used within this project are correlation coefficient and k-means clustering. With these techniques, we hope
to yield desirable results.

##Data Sets

###Boston Police Stations
This database contains information about the 12 police districts within the city of Boston. The data set contains the name
of each police district with their respective district number. Furthermore, it provides addresses, geographical coordinate,
and zip codes.

###Boston Hospitals
This data set contains information about hospitals within the city of Boston. It contains hospital names, geographical coordinates,
addresses, and zip codes. 

###Boston Crime Reports
This data set is from the city of Boston police department that contains incident reports from August 2015 to today's date.
For every entry in this data set it contains an incident number, offense code, offense description, district, date, street
address and geographical coordinates. The dat set that was collected for this analysis were from August 2015 until 18 November 2016.

###Boston Property Assessments
This data set contains tax assessment of property for the year of 2016. Each entry in this data set contains an address, zip
code, owner, mailing address, average land value, average building value, average total value, gross tax and other descriptors
for the property. 

##Data Sets

'Boston Police Stations': https://data.cityofboston.gov/resource/pyxn-r3i2.json
'Boston Crime Reports': https://data.cityofboston.gov/resource/29yf-ye7n.json
'Boston Property Assessments': https://data.cityofboston.gov/resource/g5b5-xrwi.json
'Boston Hospital Locations': https://data.cityofboston.gov/resource/u6fv-m8v4.json

##Description
Using the data sets above, I wanted to observe if there was a correlation between number of crimes within a police 
district and the average property value within that district. My hypothesis is that there should be a negative correlation 
with property value increasing and number of crimes decreasing. I will be using the Boston Police Stations, Boston Crime 
Reports and Property Assessments data sets to calculate the correlation coefficient and p-value. I also wanted to see
if hospitals are located near areas with more crime as to make ambulances and emergency services respond efficiently.
There is no auth.json file since I did not use any keys to access any of the data sets used in this project.

##Transformations

1. getData.py obtains all of the data sets and stores them as collections within MongoDB.

2. DistrictNumCrimes.py takes in Boston Crime Reports and Boston Police Stations data sets and it computes the total 
number of crimes for each district from the Boston Crime Reports and adds a new column within the Boston Police Stations
 data set called num_crimes that corresponds to its specific district.

3. PropertyMean.py takes the Boston Property Assessment and Boston Police Stations data sets and transforms them to obtain
the average property value within a 1 mile range from each police district. I used a package called pyzipcode that must
be installed in order to run this file. pyzipcode provides a database of zipcodes and functions that provide nearby
zip codes within a range of a specific zipcode you are focused on. Although the full installation of pyzipcode might not
fully install it should be able to still install pyzipcode without sqlite3.

##Correlation of Property Value and Crime
In order to determine a relationship between property value and crime rates, a method that could be used is correlation.
Correlation coefficient is a measure that determines the degree to which two variables movements are associated/related.
The range of values for the correlation coefficient are between -1 and 1. From computing the coefficient value it can yield
a positive, negative or no correlation relationship. A positive correlation is when one variable increases along with the other
variable or one variable decreases along with the other variable. For a negative correlation indicates that when one variable
increases the other variable decreases. As for no correlation, there is no relationship between those two variables. 

Before performing the analysis, we predicted that there would be a negative correlation. As property value increases then crime rates would decrease.
We assumed that property value increases in regards to safety leading to less crimes within that area. In addition, we assumed that low income areas may
have more crimes as a means to afford living. We decided to group property values and crimes within the police districts. These groupings will help provide
samples from different areas around boston in order to calculate correlation. Property values however do not have an attribute for police districts.
In order to account for this we decided to use a python package called pyzipcode which would take the zip codes from each entry in the property data set and it will
find the nearest police district within its one mile radius range. Once found the property value will be added to the closest police district. After iterating through
the property data set, we computed the average for all property value in each district. Using the average property value and sum of crimes for each district, we applied
the pearson correlation coefficient algorithm and used multiple permutations. 

Based on the calculations it yielded these results.

Correlation Coefficient: -0.3138711
P-value: 1.0

![Property and Crime Correlation graph](/ggelinas/crime_prop_corr.png)

This correlation coefficient shows that it is a moderate/weak negative correlation. With a high p-value, it states that this
observation is a non-significant result that will not prove the Null hypothesis false.
This suggests that the Null hypothesis: "Crimes decreasing and property value increasing" may be true.


##Hospital Location Analysis
Another inquiry is to determine if hospitals are optimally located against crime. We believe
that hospitals may be located around areas with crime in order to reduce time and increase efficiency in treating victims.
We decided to use the k-means algorithm to compute the difference. The k-means algorithm takes in geographical coordinates from
the crimes data set and clusters points to form k clusters determined by the input. For this experiment, we used the number of hospitals
in the hospitals data set as the k value in order to compare distances. After running the k-means algorithm and computing the difference in
distance between current hospitals and optimal hospitals it clearly showed very little difference.

Here are the results:

Locations of current hospital locations: [(42.30025000839615, -71.10737910445549), 
(42.3438499996779, -71.08983000035408), (42.31856289432221, -71.09165569529381), (42.329611374844326, -71.10616871232227),
(42.27137912172521, -71.08168028446168), (42.3371094801158, -71.07139912234962), (42.329611374844326, -71.10616871232227),
(42.36297141612903, -71.07043169540236), (42.34665771451756, -71.14136122385321), (42.33587602903896, -71.10741054246668),
(42.27854306401838, -71.06631280050811), (42.349946522039204, -71.0634111017112), (42.30021828265608, -71.12789683059322),
(42.349656455743144, -71.14822103232248), (42.27598935537618, -71.17245195460838), (42.36327718561898, -71.0668523937257),
(42.337592548462226, -71.10472284437952), (42.31617666213941, -71.11272670363542), (42.29738386053219, -71.13150465441208),
(42.314030311294516, -71.06406449543488), (42.36764789068138, -71.06564730220646), (42.352620000312925, -71.13281000028115),
(42.3385289546495, -71.10940050507557), (42.33734993862189, -71.1071702648531), (42.36247485742686, -71.06924724545246),
(42.335925371008436, -71.07378404269969)]

K means hospital locations: [(42.31766665432098, -71.08874154320986), (42.335336941176465, -71.06332752941175), 
(42.2758958548387, -71.09411869354837), (42.28982995238095, -71.10348738095237), (42.3534265, -71.0765595), 
(42.34568411111111, -71.14137922222223), (42.33503033333333, -71.10632700000001), (39.704630671641794, -66.88389731343281), 
(42.3537191, -71.13231374999998), (42.31140083760684, -71.0637535213675), (42.334351783333325, -71.07737868333335), 
(42.340216, -71.109561), (42.37631418367348, -71.04442244897959), (42.34869166666667, -71.05903538562089), 
(42.296395600000004, -71.1229084), (42.3494088, -71.15434520000002), (42.26494976923077, -71.15218800000001), 
(42.36074204166666, -71.06050870833333), (42.336705375, -71.09906812499999), (42.316876400000005, -71.11061939999999), 
(42.27484629166666, -71.13036083333334), (42.327336736842106, -71.10366789473683), (42.344890825, -71.08917249999999), 
(42.355409333333334, -71.07107333333333)]

Difference between hospitals and optimal K means clusters: [(0.017416645924832608, 0.01863756124562599), 
(0.008513058501435466, 0.026502470942332934), (0.04266703948351136, 0.0024629982545576468), 
(0.03978142246337768, 0.0026813313698994534), (0.08204737827478681, 0.005120784461681183), 
(0.008574630995312305, 0.069980099872609), (0.005418958489002534, 0.00015828767773484742), 
(2.6583407444872336, 4.186534381969551), (0.007061385482437288, 0.009047473853229349), 
(0.02447519143211707, 0.043657021099178905), (0.05580871931494613, 0.011065882825235462), 
(0.00973052203920588, 0.046149898288803115), (0.07609590101739627, 0.08347438161362675), 
(0.0009647890764767908, 0.08918564670159412), (0.020406244623821124, 0.049543554608376894), 
(0.013868385618984291, 0.08749280627432654), (0.07264277923145812, 0.047465155620486144), 
(0.04456537952724915, 0.052217995302086706), (0.03932151446780807, 0.03243652941209518), 
(0.0028460887054890804, 0.04655490456511302), (0.09280159901472018, 0.06471353112688405), 
(0.02528326347081844, 0.029142105544323726), (0.006361870350502841, 0.020228005075580313), 
(0.018059394711443133, 0.03609693151976501)]

![Optimal Hospital locations](/ggelinas/optimalhospitals.png)

##Results
In our hypothesis, we assumed in finding a negative correlation with the average property
value and the number of crimes in each district. The results displayed a moderate to weak
correlation between those two variables in addition to a high p-value. The weak correlation may
be a result of not having enough crime data as the sample was only taken from August 2015 through
November 2016. There are more data sets of crime incidents provided by Boston and there may be different
results in adding other years as well. With this negative correlation, we can assume that these variables
may have moderate relationship in affecting each other. The high p-value suggests that it would support the
hypothesis.
In regards to hospital locations, 


##Conclusion


With these results, we can conclude that 