import googlemaps
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import time
import ast


api_key='AIzaSyCKwiWXDPTAHdUFPS9UOQ732-gJ3dCta9w'
gmaps = googlemaps.Client(key=api_key)
google_places=gmaps.places(query='Boston Parking', location='Boston, MA')


pg2='CuQB2wAAAOC0gaeKskZdoXlL0IEZQdq6Goh5V3co-2lV5nXMBn3Xm0-Dfc4NIv0ZTF59i67rb7pYqSYccWnSemirZJm413GwXadgJ6Qp5AZ6FcYEWguNRB1rf3aqAPkafHhLtz14CHnUuXa1MD_xh9dR-ElBR_3zoKRncnAXAC-F0xVKqr-neWXF6RdbpeASczCFjCjFB9aK-8HbZodiGk9siKtm3Cl3Dcc7bv67S_OrIu5PCv55mTQR0cgZgypGcz6ctcwRtZ4G0f9U9cqNajZq2ZYn5peh9P4fMkTfub-sCr7v0iFEEhB___HE7nGPkGsBQg8argV0GhSgF428vvpKPEzHKk__ExzaWGCuzw'
google_places_1=gmaps.places(query='Boston Parking', location='Boston, MA', page_token=pg2)

pg3='CoQDewEAAE-PjFnvJBFH81S5UIGvcthwgD-XauRh9YLD0JRviv5TpiqE7XRAjtRL4igFEMcTSJ_ArAy2OetcEx3W-y9lCRWr9Ghbd2w3yVgBbIBeewe5FsnulsdbRDHrfrSjDOpyzODLLgbueFCE_tcZR8czzmHFf6RwnvyVjdMkFDCr4MgyCSgZZOI5bUWfujMrT6XxhvYACiuwQQVFPjSFP9dxLhO88RgBIV1LHhrK0loruQcH04adVWtzrL04chUVNhlSiL6wv2j_FinWun_ulKwkRegGCfLQGVezgDZ6kZ3LoaRoiyYkmI2qbWrXbPIcqDDJ_tqfJd71U0hRu1NcWavMIUwIcnM1LC99rRSFSX90mo2YeHP5KIC1BBtDSiJ_XP2iUgO82vtz5kzitSbMENf8DmmaFIux1HkHHXvp7IIfhV4I7Jxj4m3B2qKHqsZjL2l8a4W0Rk7Pwkg-qD3EKYIPqUzBcMEIB4L5Q5bT6XOsla3t9AxEMgUPW_1YXCrCtLCPPxIQmeCwG4TP_O08PU8hCW57CxoUXsWiL7vazToAWANu54OF0DkhZhw'

google_places_2=gmaps.places(query='Boston Parking', location='Boston, MA', page_token=pg3)

#for elem in google_places_1['results']:
#	print(elem['name'])	
































