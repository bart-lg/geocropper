# Description

The **geocropper** Python package provides download and crop/clip functionality for Sentinel-1 (currently download only), Sentinel-2, Landsat-TM, Landsat-ETM and Landsat-8 products/tiles.

# Usage

## Dependencies

## Python

```python
from geocropper import *
geocropper.importAllCSVs()
geoc = geocropper.init(16, 48)
geoc.downloadSentinelData(...)
geoc.downloadLandsatData(...)
geoc.downloadAndCrop(...)
```

## CSV

Downloading and cropping tiles based on csv files with geolocations located in directory data/csvInput.

Tiles will be downloaded to data/bigTiles.  
Cropped tiles will be saved in data/croppedTiles.  
Loaded csv files will be moved to data/csvArchive.  

CSV files can be imported through python or shell.

```
make importall
```

### CSV Structure

default csv delimiter: ,  
default csv quotechar: "

#### Mandatory fields

Fields | Description
---|---
lat | Latitude of geolocation.
lon | Longitude of geolocation.
dateFrom | Start date for search request in a chosen format.<br />The format must be recognizable by the [dateutil](https://dateutil.readthedocs.io/) lib.<br />In case of doubt use the format 'YYYY-MM-DD'.
dateTo | End date for search request in a chosen format.<br />The format must be recognizable by the dateutil lib.<br />In case of doubt use the format 'YYYY-MM-DD'.
platform | Choose between 'Sentinel-1', 'Sentinel-2', <br />'LANDSAT_TM_C1', 'LANDSAT_ETM_C1' and 'LANDSAT_8_C1'.

#### Optional fields

Fields | Description
---|---
width | Width of cropped rectangle.
height | Heigth of cropped rectangle.
tileLimit | Maximum number of tiles to be downloaded.
description | Description for the request to be stored in the sqlite database.
cloudcoverpercentage | Value between 0 and 100 for maximum cloud cover percentage.
producttype | Sentinel-1 products: RAW, SLC, GRD, OCN<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;SLC: Single Look Complex<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;GRD: Ground Range Detected<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;OCN: Ocean<br />Sentinel-2 products: S2MSI1C, S2MSI2A, S2MSI2Ap
polarisationmode | Used for Sentinel-1 products:<br />Accepted entries are: HH, VV, HV, VH, HH+HV, VV+VH
sensoroperationalmode | Used for Sentinel-1 products:<br />Accepted entries are: SM, IW, EW, WV<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;SM: Stripmap<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;IW: Interferometric Wide Swath<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;EW: Extra Wide Swath<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;WV: Wave
swathidentifier | Used for Sentinel-1 products:<br />Accepted entries are: S1, S2, S3, S4, S5, S6, IW, IW1, IW2, IW3, EW, EW1, EW2, EW3, EW4, EW5
timeliness | Used for Sentinel-1 products:<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;NRT: NRT-3h (Near Real Time)<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;NTC: Fast-24h

The package omits all other field names.

# Country determination

Data for country borders obtained from: http://thematicmapping.org/downloads/world_borders.php
