# Geocropper

## Description

The **geocropper** Python package provides download and crop/clip functionality for Sentinel-1 (currently download only), Sentinel-2, Landsat-TM, Landsat-ETM and Landsat-8 products/tiles.

## Usage

### CSV

Downloading and cropping tiles based on csv files with geolocations located in directory data/csvInput.

Tiles will be downloaded to data/bigTiles
Cropped tiles will be saved in data/croppedTiles
Loaded csv files will be moved to data/csvArchive

```
make importall
```

#### CSV Structure

default csv delimiter: ,  
default csv quotechar: "

##### Mandatory fields

* lat
* lon
* dateFrom
* dateTo
* platform

##### Optional fields

* width
* height
* tileLimit
* description
* cloudcoverpercentage
* producttype
* polarisationmode
* sensoroperationalmode
* swathidentifier
* timeliness

The package omits all other field names.

## Country determination

Data for country borders obtained from: http://thematicmapping.org/downloads/world_borders.php
