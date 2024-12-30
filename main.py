import base64
import ssl

import geopy.distance
import googlemaps
import pandas as pd
import requests
from datetime import datetime
from geopy.geocoders import Nominatim
import folium
from folium.plugins import MarkerCluster
import math
import pylab
from shapely.geometry import Point
from pyproj import Transformer
from shapely.ops import transform
from bs4 import BeautifulSoup
from urllib.request import urlopen
from shodan import Shodan

pd.set_option('display.max_columns', None)  # or 1000
pd.set_option('display.max_rows', None)  # or 1000
pd.set_option('display.max_colwidth', None)

GOOGLE_API_KEY= ""
SHODAN_API_KEY=""

#It gathers all the points defining the circunference around (latitude,longitude) with radius equal to "error"
def get_points_circunference(latitude,longitude,error):
    local_azimuthal_projection = "+proj=aeqd +R=6371000 +units=m +lat_0={} +lon_0={}".format(latitude, longitude)
    wgs84_to_aeqd = Transformer.from_proj('+proj=longlat +datum=WGS84 +no_defs', local_azimuthal_projection)
    aeqd_to_wgs84 = Transformer.from_proj(local_azimuthal_projection, '+proj=longlat +datum=WGS84 +no_defs')
    point_transformed = Point(wgs84_to_aeqd.transform(latitude, longitude))
    buffer = point_transformed.buffer(error)
    circle = transform(aeqd_to_wgs84.transform, buffer)
    return list(circle.exterior.coords)

#It collects all the points each "distance" meters in the circunference around the coordinates
def get_all_points_around(latitude,longitude,error,distance):
    res=[]
    for i in range(distance,error+distance,distance):
        for p in get_points_circunference(latitude,longitude,i):
            res.append(p)
    return res

#It filters the points to keep only one each "distance" meters
def filter_points_distance(points,distance):
    res=[]
    last_point_added=(0,0)
    for p in points:
        dif=geopy.distance.geodesic(p, last_point_added).m
        if dif > distance:
            res.append(p)
            last_point_added=p
    return res

def generate_youtube_search(latitude,longitude):
    return "https://mattw.io/youtube-geofind/location?location="+str(latitude)+","+str(longitude)+"&radius=5&doSearch=true"

def generate_twitter_search(latitude,longitude):
    return "https://twitter.com/search?q=near%3A"+str(latitude)+"%2C"+str(longitude)+"&src=typed_query&f=live"

#def generate_twitter_search(latitude,longitude,date_init,date_end):
#    return "https://twitter.com/search?q=near%3A"+str(latitude)+"%2C"+str(longitude)+"%20until%3A"+str(date_end)+"%20since%3A"+str(date_init)+"&src=typed_query&f=live"

def get_catastral_information(latitude,longitude):
    url="https://www1.sedecatastro.gob.es/CYCBienInmueble/OVCListaBienes.aspx?del=30&muni=28&rc1=0020006&rc2=00WH72F&from=OVCBusqueda&final=&pest=coordenadas&latitud="+str(latitude)+"&longitud="+str(longitude)+"&gradoslat=&minlat=&seglat=&gradoslon=&minlon=&seglon=&x=&y=&huso=0&tipoCoordenadas=2&TipUR=Coor"

    gcontext = ssl.SSLContext()
    page = urlopen(url, context=gcontext)
    html = page.read().decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    bad_string = "  copiar       código de barras"

    tags = soup.findAll('label', {'class': lambda x: x and 'control-label black text-left' in x})

    res=[]
    for s in tags:
        res.append(s.get_text(separator=" ").strip().replace(bad_string, ""))
    res=[res[0],res[1]]
    return res

def get_maps_image(latitude,longitude):
    url="https://maps.googleapis.com/maps/api/staticmap?center=" + str(latitude) +",+" + str(longitude) +"&zoom=17&scale=1&size=600x300&maptype=satellite&format=png&visual_refresh=true&key=" + GOOGLE_API_KEY
    img = requests.get(url, stream=True)
    str_equivalent_image = base64.b64encode(img.content).decode()
    img_tag = "<img src='data:image/png;base64," + str_equivalent_image + "'/>"
    return img_tag


def get_maps_address(latitude,longitude):
    gmaps = googlemaps.Client(key=GOOGLE_API_KEY)
    reverse_geocode_result = gmaps.reverse_geocode((latitude, longitude))
    return reverse_geocode_result[0]["formatted_address"]


def get_nearby_places(latitude,longitude,distance):
    gmaps = googlemaps.Client(key=GOOGLE_API_KEY)
    reverse_geocode_result = gmaps.places_nearby(location=(latitude, longitude), radius=distance)
    res=[]
    for r in reverse_geocode_result["results"]:
        res.append((r["name"]))
    return res

def shodan_search(term):
    api = Shodan(SHODAN_API_KEY)
    r = api.search(term)
    url = "https://www.shodan.io/search?query=" + term

    res = {}
    res["Unknown"] = []
    for i in r["matches"]:
        if ("product" in i.keys()):
            if i["product"] not in res.keys():
                res[i["product"]] = []
            res[i["product"]].append(i["ip_str"])
        else:
            res["Unknown"].append(i["ip_str"])
    return url,res;

def generate_shodan_html(url,res):
    html="<a href="+url+">Shodan</a><br>"
    #html+='<button type = "button" class ="collapsible" > + </button >'
    #html += '<div class ="content">'
    #for k in res.keys():
     #   html += '<button type = "button" class ="collapsible" >'+str(k + " " + str(len(res[k])))+'</button >'
      #  html += '<div class ="content">'
       # html += '<p>'+str(res[k])+'</p>'
        #html += '</div>'
    #html+='</div>\n'
    #html += '<script> var coll = document.getElementsByClassName("collapsible"); var i; for (i = 0; i < coll.length; ' \
     #       'i++) { coll[i].addEventListener("click", function() {this.classList.toggle("active"); var content = ' \
      #      'this.nextElementSibling; if (content.style.display === "block") {content.style.display = "none"; } else ' \
       #     '{content.style.display = "block";}});}</script> '
    return html


if __name__ == '__main__':
    gmaps = googlemaps.Client(key=GOOGLE_API_KEY)
    geolocator = Nominatim(user_agent="example app")

    #latitude=38.172815
    #longitude=-2.094096

    latitude=38.25838168550172
    longitude=-0.7136876813937173

    error= 200
    distance_between_circles=200
    distance_between_points=100

    location=(latitude,longitude)


    all_points=filter_points_distance(get_all_points_around(latitude,longitude,error,distance_between_circles),distance_between_points)
    print(all_points)


    #Drawing part

    df_external_circle = pd.DataFrame(get_points_circunference(latitude, longitude, error), columns=["lat", "lon"], index=None)
    m = folium.Map(location=df_external_circle[["lat", "lon"]].mean().to_list(), zoom_start=16)
    marker_cluster = MarkerCluster().add_to(m)

    list_locations=[]
    for i, r in df_external_circle.iterrows():
        location2 = (r["lat"], r["lon"])
        list_locations.append(location2)

    #Reorder list of coordinates to draw perimeter
    cent = (sum([p[0] for p in list_locations]) / len(list_locations), sum([p[1] for p in list_locations]) / len(list_locations))
    list_locations.sort(key=lambda p: math.atan2(p[1] - cent[1], p[0] - cent[0]))
    folium.Polygon(list_locations, fill_color="yellow", fill_opacity=0.3).add_to(m)



    text_popup="<b>"+str(latitude)+","+str(longitude)+"</b><br>"
    text_popup +="<br>"

    text_popup +="<b>Google Maps Address:</b><br>"
    google_addres=get_maps_address(latitude,longitude)
    text_popup+=google_addres+"<br>"
    text_popup+="<br>"

    text_popup +="<b>Catrastal information:</b><br>"
    catastral_data=get_catastral_information(latitude,longitude)
    for i in catastral_data:
        text_popup+=i+"<br>"
    text_popup +="<br>"

    text_popup+="<b>Nearby interesting places:</b><br>"
    places=get_nearby_places(latitude,longitude,error)
    for i in range(0,3,1):
        if len(places)>i:
            text_popup += places[i] + "<br>"
    text_popup += "<br>"

    url_shodan,list_shodan=shodan_search(places[0])
    text_popup+=generate_shodan_html(url_shodan,list_shodan)

    twitter_search=generate_twitter_search(latitude,longitude)
    text_popup += "<a href="+twitter_search + ">Tweets</a><br>"

    youtube_videos=generate_youtube_search(latitude,longitude)
    text_popup += "<a href="+youtube_videos + ">Youtube Videos</a><br>"

    print(text_popup)

    img_tag=get_maps_image(latitude,longitude)
    text_popup+=img_tag+"<br>"

    folium.Marker(location=location,popup=text_popup).add_to(m)  # display the map

    all_points=filter_points_distance(get_all_points_around(latitude,longitude,error,distance_between_circles),distance_between_points)
    for p in all_points:
        folium.Marker(location=p, popup=str(p)).add_to(m)  # display the map


    m.save("map.html")
