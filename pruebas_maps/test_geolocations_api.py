t= "calle 16 a # 2-16" #; carrera 80 # 8c-85"
t= "calle 16 a # 2-16, bogota" # si funciona
#t= ["carrera 80 # 8c-85", "calle 16 a # 2-16, bogota"] solo coje la primera direccion pero la otra la coje de a pedazos pero solo da informacion y cordenadas de la primera
t= """carrera 80 # 8c-85
calle 16 a # 2-16"""

#pip install -U googlemaps
import googlemaps

gmaps = googlemaps.Client(key=open("maps_api_key.txt").read())

if False:
  # Geocoding an address
  geocode_result = gmaps.geocode(t
  #,language= "es"
  #, region= ".co"
  )
  for r in geocode_result:
    print(r["formatted_address"], r["geometry"]["location"]["lat"], r["geometry"]["location"]["lng"])
    print("----")


################

s2= gmaps.addressvalidation(t
#,language= "es"
#, region= ".co"
)

#print(s2["result"]["address"]["formattedAddress"]
# s2["result"]["geocode"]["location"]["latitude"],
# s2["result"]["geocode"]["location"]["longitude"])
print(s2)