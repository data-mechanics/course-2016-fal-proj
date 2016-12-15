#Boston Crime Rates in Relation to Hospitals, Police Districts and Property

##Introduction
In this project, using data from the city of Boston we seek to understand
crime rates in relation to hospitals, police districts, and property. We
wanted to determine:
<ol>
    <li>If crime were more likely in areas of lower property values
    <li>Are hospitals optimally located around crime areas
</ol>



# course-2016-fal-proj-2 (ggelinas)
Project repository for the course project in the Fall 2016 iteration of the Data Mechanics course at Boston University.

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

2. Within DistrictNumCrimes.py I take Boston Crime Reports and Boston Police Stations data sets and take the total 
number of crimes for each district from the Boston Crime Reports and add a new column within the Boston Police Stations
 data set called num_crimes that corresponds to its specific district.

3. PropertyMean.py takes the Boston Property Assessment and Boston Police Stations data sets and transforms them to obtain
the average property value within a 1 mile range from each police district. I used a package called pyszipcode that must
be installed in order to run this file. pyzipcode provides a database of zipcodes and functions that provide nearby
zip codes within a range of a specific zipcode you are focused on. Although the full installation of pyzipcode might not
fully install it should be able to still install pyzipcode without sqlite3.

##Property Crime Analysis

Before the analysis, I predicted that there would be a negative correlation coefficient as number of crimes going down
and property value increasing within police districts. Based on the calculations it yielded these results.

Correlation Coefficient: -0.6809827474468138
P-value: 1.0

This correlation coefficient shows that it is a strong negative correlation.

##Hospital Location Analysis
This analysis is to determine if the current hospital locations are placed in optimal locations by comparing it towards
K means of the 26 hospitals. The algorithm takes in the Boston Hospital Locations and Boston Crime Reports data set.
It compares the current location of the hospitals and compares the distance for each location with the most crimes.

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

Analyzing the difference in the K means algorithm and the current locations sow that the locations of the Hospitals are
not far off from the optimal locations determined by the most number of crimes. 