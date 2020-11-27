# -*- coding: utf-8 -*-
"""
Python script to convert coordinate column from csv file
from "46° 05′ 58″ N; 5° 20′ 56″ E" format to (46.1, 5.35) Lat/Lng format

Contributors: DAUPHIN Yohan - GIRERD Thomas - LAI-KING Mathieu - VILIN Victor
Date: 11/21/2020
"""
import csv
#https://fr.wikipedia.org/wiki/Liste_des_centres_de_gravit%C3%A9_des_d%C3%A9partements_m%C3%A9tropolitains_fran%C3%A7ais

def formatLatLong(s):
    for c in ["°", "′", "″"]:
        s = s.replace(c, "")
    s = s.split(", ")
    lat, long = s[0], s[1]
    N = 'N' in lat
    d, m, s = map(float, lat.split(' ')[:-1])
    lat = (d + m / 60. + s / 3600.) * (1 if N else -1)
    W = 'O' in long
    d, m, s = map(float, long.split(' ')[:-1])
    long = (d + m / 60. + s / 3600.) * (-1 if W else 1)

    return float("{:.5f}".format(lat)), float("{:.5f}".format(long))

out = []
with open ("coords.csv", "r", encoding="utf-8") as f:
    reader = csv.reader(f, delimiter=';')
    i = -1
    for row in reader:
        i+=1
        if i >= 2:
            row += formatLatLong(row[3])
        elif i == 0:
            row += ["Lat", "Long"]
        else:
            continue
        row = row[:3] + row[4:]
        out.append(row)

with open("coords_formatted.csv", "w", encoding="utf-8", newline='') as f:
    writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    for row in out:
        print(row)
        writer.writerow(row)