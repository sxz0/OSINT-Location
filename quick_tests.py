import ssl
from bs4 import BeautifulSoup
from urllib.request import urlopen
import googlemaps
from shodan import Shodan

#latitude = 38.263336
#longitude = -0.737174

latitude = 38.263386
longitude = -0.7372311
address="Av. de ľ Alcalde Ramón Pastor, 2, 03204 Elche, Alicante, Spain"

SHODAN_API_KEY=""
API_KEY=""

gmaps = googlemaps.Client(key=API_KEY)

google_address=gmaps.reverse_geocode((latitude,longitude))

print(google_address[0]["address_components"][3]["long_name"])

term=google_address[0]["address_components"][3]["long_name"]

api = Shodan(SHODAN_API_KEY)

url="https://www.shodan.io/search?query="+term

r=api.search(term)

res={}
res["Unknown"]=[]
for i in r["matches"]:
    if("product" in i.keys()):
        if i["product"] not in res.keys():
            res[i["product"]]=[]
        res[i["product"]].append(i["ip_str"])
    else:
        res["Unknown"].append(i["ip_str"])

print(res)
for k in res.keys():
    print(k+" "+str(len(res[k])))
    print(res[k])
    print()



