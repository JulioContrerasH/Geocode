import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time

def revert_address(address):
    parts = address.split(',')
    if len(parts) > 0:
        return ','.join(reversed(parts))
    else:
        return address

df = pd.read_excel('D:/CURSOS_2022/Repos/Geocode/data/direcciones.xlsx')
df['DIR CONCA'] = df['DIR CONCA'].apply(revert_address)

formatted_addresses = []

for index, row in df.iterrows():
    print(row)
    address_parts = [
        row['DIR CONCA'], 
        row['DISTRITO'], 
        row['PROVINCIA'], 
        row['REGION'], 
        'Perú'  # País en este caso es siempre 'Peru' según tus datos
    ]
    formatted_address = ','.join(address_parts)
    formatted_addresses.append(formatted_address)

for address in formatted_addresses:
    print(address)

def obtener_coordenadas_desde_direccion(direccion):
    geolocator = Nominatim(user_agent="my_geocoder")

    intentos_maximos = 3
    intento_actual = 1
    while intento_actual <= intentos_maximos:
        try:
            location = geolocator.geocode(direccion, timeout=10)
            if location:
                return location.latitude, location.longitude
            else:
                return None
        except Exception as e:
            print(f"Intento {intento_actual} falló. Error: {e}")
            intento_actual += 1
            time.sleep(2)  # Esperar un poco antes de reintentar

    print("No se pudieron obtener las coordenadas después de varios intentos.")
    return None

 
index = -1
for address in formatted_addresses:
    index += 1
    direccion_parts = address.split(',')
    eli = 0
    while direccion_parts:
        eli += 1
        dir = ','.join(direccion_parts)
        coordenadas = obtener_coordenadas_desde_direccion(dir)
        if coordenadas is not None:
            df.at[index, "DirUsada"] = dir
            df.at[index, "X"] = coordenadas[0]
            df.at[index, "Y"] = coordenadas[1]
            df.at[index, "Eliminado"] = eli
            break
        else:
            print("Intentando con dirección:", ','.join(direccion_parts))
            direccion_parts.pop(0)
    
    
archivo_salida = 'D:/CURSOS_2022/Repos/Geocode/data/direcciones_modificado.xlsx'
df.to_excel(archivo_salida, index=False) 