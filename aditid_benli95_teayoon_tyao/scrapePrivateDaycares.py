'''
Selenium 2.53.1
Python 3.4.4
'''
import time
import selenium
from selenium import webdriver
from geopy.geocoders import Nominatim

def getData():
	start_time = time.time()
	print("Scraping private daycares")
	driver = webdriver.Chrome("/Users/aditi/Desktop/cs591L/course-2016-fal-proj/aditid_benli95_teayoon_tyao/chromedriver")
	geolocator = Nominatim()
	driver.get("https://www.care.com/day-care/boston-ma-page")

	data = {}
	while True:
		daycares = driver.find_elements_by_class_name(" individual-info")
		for daycare in daycares:
			daycareName = daycare.find_element_by_class_name(' pro-title').text.replace(".", "")
			daycareAddress = daycare.find_element_by_class_name('cg-desc-short').text.split("\n")[0]
			try:
				location = geolocator.geocode(daycareAddress)
				longitude = location.longitude
				latitude = location.latitude
				data[daycareName] = {"longitude": longitude, "latitude": latitude}
			except:
				pass
		try:
			nextURL = driver.find_element_by_xpath("//*[@rel='next']").get_attribute('href')
			driver.get(nextURL)
		except:
			break

	driver.close()
	elapsed_time = time.time() - start_time
	print("Time elapsed: " + str(elapsed_time))
	print("Finished scraping private daycares")
	return data