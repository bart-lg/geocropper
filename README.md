# Description

The **geocropper** Python package provides download and crop/clip functionality for Sentinel-1 (currently download only), Sentinel-2, Landsat-TM, Landsat-ETM and Landsat-8 products/tiles.

# Requirements

- Python 3.6
- Anaconda
- Copernicus Open Access Hub Account (see Usage/Configuration)
- USGS Earth Resources Observation and Science (EROS) System Account (see Usage/Configuration)

# Clone

**Important:**
Use the option --recurse-submodules when cloning this repository. This includes the submodule lib/countries automatically.

```
git clone --recurse-submodules https://github.com/bart-lg/geocropper
```

# Usage

## Dependencies

To install all dependencies using conda:

```
conda env update -f env.yml
```

## Configuration

Copy the file default-config.ini to user-config.ini and add your user credentials for the following platforms:
- Copernicus Open Access Hub: https://scihub.copernicus.eu/dhus/
- USGS Earth Resources Observation and Science (EROS) System: https://ers.cr.usgs.gov/register/

## Python

Activate the conda environment:

```
conda activate env
```

Import package in python:

```python
from geocropper import *
```

### Using CSV

```python
geocropper.import_all_csvs()
```

Place your CSV files in the inputCSV directory defined in the config file (default: 'data/inputCSV').
With this function all CSVs get imported and loaded.
This means that for all geolocations the appropriate tiles get downloaded and cropped according to the request.



### Using Geocropper class and functions

<br />

**init(lat, lon)**

Initialization of a Geocropper instance.

Parameter | type | description
---|---|---
lat | float | Latitude of the geolocation (WGS84 decimal).
lon | float | Longitude of the geolocation (WGS84 decimal).

```python
geoc = geocropper.init(48, 16)
```
<br /><br />
**printPosition()**

Prints current location attributes of Geocropper object to console.

<br /><br />
**downloadAndCrop(groupname, dateFrom, dateTo, platform, width, height, tileLimit = 0, \*\*kwargs)**

Download and crop/clip Sentinel or Landsat tiles to directories specified in the config file (default: 'data/bigTiles' and 'data/croppedTiles').

Parameter | type | description
---|---|---
groupname | str | Short name to group datasets (groupname is used for folder structure in cropped tiles)
dateFrom | str | Start date for search request in a chosen format.<br />The format must be recognizable by the [dateutil](https://dateutil.readthedocs.io/) lib.<br />In case of doubt use the format 'YYYY-MM-DD'.
dateTo | str | End date for search request in a chosen format.<br />The format must be recognizable by the [dateutil](https://dateutil.readthedocs.io/) lib.<br />In case of doubt use the format 'YYYY-MM-DD'.
platform | str | Choose between 'Sentinel-1', 'Sentinel-2', 'LANDSAT_TM_C1', 'LANDSAT_ETM_C1' and 'LANDSAT_8_C1'
width | int | Width of cropped rectangle. The rectangle surrounds the given geolocation (center point).
height | int | Heigth of cropped rectangle. The rectangle surrounds the given geolocation (center point).
tileLimit | int | (optional) Maximum number of tiles to be downloaded.
cloudcoverpercentage | int | (optional) Value between 0 and 100 for maximum cloud cover percentage.
producttype | str | (optional) Sentinel-1 products: RAW, SLC, GRD, OCN<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;SLC: Single Look Complex<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;GRD: Ground Range Detected<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;OCN: Ocean<br />Sentinel-2 products: S2MSI1C, S2MSI2A, S2MSI2Ap
polarisationmode | str | (optional) Used for Sentinel-1 products:<br />Accepted entries are: HH, VV, HV, VH, HH+HV, VV+VH
sensoroperationalmode | str | (optional) Used for Sentinel-1 products:<br />Accepted entries are: SM, IW, EW, WV<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;SM: Stripmap<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;IW: Interferometric Wide Swath<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;EW: Extra Wide Swath<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;WV: Wave
swathidentifier | str | (optional) Used for Sentinel-1 products:<br />Accepted entries are: S1, S2, S3, S4, S5, S6, IW, IW1, IW2, IW3, EW, EW1, EW2, EW3, EW4, EW5
timeliness | str | (optional) Used for Sentinel-1 products:<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;NRT: NRT-3h (Near Real Time)<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;NTC: Fast-24h

```python
geoc.downloadAndCrop(dateFrom = "20190701", dateTo = "20190731", platform = "Sentinel-2", width = 2000, height = 2000, tileLimit = 2, cloudcoverpercentage = 30)
```

*Note: Sentinel-1 data get only downloaded and not cropped for now. Feel free to contribute to the project if you know how to crop Sentinel-1 data.*

<br /><br />
**downloadSentinelData(dateFrom, dateTo, platform, poiId = 0, tileLimit = 0, \*\*kwargs)**

Download Sentinel tiles to directory specified in the config file (default: 'data/bigTiles').

Parameter | type | description
---|---|---
dateFrom | str | Start date for search request in a chosen format.<br />The format must be recognizable by the [dateutil](https://dateutil.readthedocs.io/) lib.<br />In case of doubt use the format 'YYYY-MM-DD'.
dateTo | str | End date for search request in a chosen format.<br />The format must be recognizable by the [dateutil](https://dateutil.readthedocs.io/) lib.<br />In case of doubt use the format 'YYYY-MM-DD'.
platform | str | Choose between 'Sentinel-1' and 'Sentinel-2'
poiId | int | (optional) ID of PointOfInterest record in sqlite database.<br />This is primarly used by other functions to create a connection between the database records.
tileLimit | int | (optional) Maximum number of tiles to be downloaded.
cloudcoverpercentage | int | (optional) Value between 0 and 100 for maximum cloud cover percentage.
producttype | str | (optional) Sentinel-1 products: RAW, SLC, GRD, OCN<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;SLC: Single Look Complex<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;GRD: Ground Range Detected<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;OCN: Ocean<br />Sentinel-2 products: S2MSI1C, S2MSI2A, S2MSI2Ap
polarisationmode | str | (optional) Used for Sentinel-1 products:<br />Accepted entries are: HH, VV, HV, VH, HH+HV, VV+VH
sensoroperationalmode | str | (optional) Used for Sentinel-1 products:<br />Accepted entries are: SM, IW, EW, WV<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;SM: Stripmap<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;IW: Interferometric Wide Swath<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;EW: Extra Wide Swath<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;WV: Wave
swathidentifier | str | (optional) Used for Sentinel-1 products:<br />Accepted entries are: S1, S2, S3, S4, S5, S6, IW, IW1, IW2, IW3, EW, EW1, EW2, EW3, EW4, EW5
timeliness | str | (optional) Used for Sentinel-1 products:<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;NRT: NRT-3h (Near Real Time)<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;NTC: Fast-24h

```python
geoc.downloadSentinelData(dateFrom = "20190701", dateTo = "20190731", platform = "Sentinel-2", tileLimit = 2, cloudcoverpercentage = 30)
```

Return value: number of found and downloaded tiles (int)


<br /><br />
**downloadLandsatData(dateFrom, dateTo, platform, poiId = 0, tileLimit = 0, \*\*kwargs)**

Download Landsat tiles to directory specified in the config file (default: 'data/bigTiles').

Parameter | type | description
---|---|---
dateFrom | str | Start date for search request in a chosen format.<br />The format must be recognizable by the [dateutil](https://dateutil.readthedocs.io/) lib.<br />In case of doubt use the format 'YYYY-MM-DD'.
dateTo | str | End date for search request in a chosen format.<br />The format must be recognizable by the [dateutil](https://dateutil.readthedocs.io/) lib.<br />In case of doubt use the format 'YYYY-MM-DD'.
platform | str | Choose between 'LANDSAT_TM_C1', 'LANDSAT_ETM_C1' and 'LANDSAT_8_C1'
poiId | int | (optional) ID of PointOfInterest record in sqlite database.<br />This is primarly used by other functions to create a connection between the database records.
tileLimit | int | (optional) Maximum number of tiles to be downloaded.
cloudcoverpercentage | int | (optional) Value between 0 and 100 for maximum cloud cover percentage.

```python
geoc.downloadLandsatData(dateFrom = "20190701", dateTo = "20190731", platform = "LANDSAT_8_C1", tileLimit = 2, cloudcoverpercentage = 30)
```
<br />

## CSV

Downloading and cropping tiles based on csv files with geolocations located in directory data/csvInput.

Tiles will be downloaded to data/bigTiles.  
Cropped tiles will be saved in data/croppedTiles.  
Loaded csv files will be moved to data/csvArchive.  

CSV files can be imported through python or shell.

Activate the conda environment:

```
conda activate env
```

Run import using [Make](https://www.gnu.org/software/make/):

```
make importall
```

### CSV Structure

default csv delimiter: ,  
default csv quotechar: "

#### Mandatory fields

Fields | Description
---|---
groupname | Short name to group datasets.<br />(groupname is used for folder structure in cropped tiles)
lat | Latitude of geolocation (WGS84 decimal).
lon | Longitude of geolocation (WGS84 decimal).
dateFrom | Start date for search request in a chosen format.<br />The format must be recognizable by the [dateutil](https://dateutil.readthedocs.io/) lib.<br />In case of doubt use the format 'YYYY-MM-DD'.
dateTo | End date for search request in a chosen format.<br />The format must be recognizable by the [dateutil](https://dateutil.readthedocs.io/) lib.<br />In case of doubt use the format 'YYYY-MM-DD'.
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

For automatic country determination the following tool is used: [countries](https://github.com/che0/countries)  
Data for country borders obtained from: http://thematicmapping.org/downloads/world_borders.php

# Acknowledgement
We gratefully acknowledge support from the European Research Council (“reFUEL” ERC2017-STG 758149).
