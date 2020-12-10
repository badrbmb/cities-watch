import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SUMMARY_FOLDER = os.path.join(ROOT_DIR, 'data', 'run_summaries')
os.makedirs(SUMMARY_FOLDER, exist_ok=True)
RESUlTS_FOLDER = os.path.join(ROOT_DIR, 'data', 'results')
os.makedirs(RESUlTS_FOLDER, exist_ok=True)
SCHEMA_FOLDER = os.path.join(ROOT_DIR, 'data', 'schemas')
os.makedirs(SCHEMA_FOLDER, exist_ok=True)
FAILS_FOLDER = os.path.join(ROOT_DIR, 'data', 'fails')
os.makedirs(FAILS_FOLDER, exist_ok=True)
OSM_NODES_FOLDER = os.path.join(ROOT_DIR, 'data', 'osm_nodes')
os.makedirs(OSM_NODES_FOLDER, exist_ok=True)
"""
Path to root repository
"""

VERBOSE = True
"""
Boolean to controls the verbosity of the code
"""

UPDATE = False
"""
Boolean to choose whether or not to update existing files on bucket
"""

BUFFER_COEFF = 0.1
SIMPLIFY_COEFF = 0.1
MAX_AREA = 50
MIN_SAMPLE_POLYGONS = 1
RETURN_SINGLES = False
"""
Coefficients to control the split of big aois
RETURN_SINGLES to choose to return individual polygons or multipolygons 
"""

MS_BANDS_CORRESPONDENCE = {'LANDSAT/LC08/C01': {'B2': 'blue',
                                                'B3': 'green',
                                                'B4': 'red',
                                                'B5': 'nir',
                                                'B6': 'swir1',
                                                'B7': 'swir2'},
                           'LANDSAT/LE07/C01': {'B1': 'blue',
                                                'B2': 'green',
                                                'B3': 'red',
                                                'B4': 'nir',
                                                'B5': 'swir1',
                                                'B7': 'swir2',
                                                },
                           'LANDSAT/LT05/C01': {'B1': 'blue',
                                                'B2': 'green',
                                                'B3': 'red',
                                                'B4': 'nir',
                                                'B5': 'swir1',
                                                'B7': 'swir2',
                                                }
                           }
MS_LEVEL = 'T1_SR'
REF_MS_COLLECTION = "LANDSAT/LC08/C01/T1_SR"
RGB_bands = ['red', 'green', 'blue']  # RGB
NDVI_bands = ['nir', 'red']  # NDVI
MAX_CLOUD_COVER = 20
"""
Bands names from multi-spectral imagery and max cloud cover filtering threshold
"""
CONTROL_SHAPE = {'geodesic': False,
                 'type': 'Polygon',
                 'coordinates': [[[-115.514778, 35.857135],
                                 [-115.514778, 36.402322],
                                 [-114.724376, 36.402322],
                                 [-114.724376, 35.857135],
                                 [-115.514778, 35.857135]]]}
REF_RAD_COLLECTION = 'NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG'
COMMON_RANGE_NTL = ['2012-04-01', '2014-01-01']
"""
Parameters for the nighttime lights imagery 
"""

CITY_THRESHOLD = .9
"""
Probability threshold to convert prediction to binary class image
"""

TILESCALE = 16
SCALE_MULTI = 4
"""
Splitting the inputs into the number of tiles=TILESCALE**2
SCALE_MULTI is a multiplier to up-sample the final shapes to  SCALE_MULTI*SCALE meters / pixel
"""

PROJECT_NAME = os.getenv("PROJECT_NAME")
MODEL_NAME = 'fcnn_e20_ts16000_bc16_es8000_px250_model'
MODEL_VERSION = 'v0'
INPUT_TILE_SIZE = [144, 144]
INPUT_OVERLAP_SIZE = [8, 8]
SCALE = 250
RESPONSE = 'classes'
"""
Deep model parameters as hosted on AI platform 
"""

AWS_ACCESS_KEY_ID = os.getenv('GS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.getenv('GS_SECRET')
BUCKET_NAME = 'dev-viirs-dnb'
BUCKET_REGION = 'eur4'
END_POINT_URL = 'https://storage.googleapis.com'
FOLDER = 'cities_shapes'
FILENAME = 'predicted_shapes'
"""
Bucket where city vectors are stored (GeoJSON)
"""

POP_THRESHOLD = 50
"""
Threshold of max population count by grid to convert population density image to binary image of human-settlements 
Based on granularity of GHSL (Global Human Settlement Layers) dataset
https://developers.google.com/earth-engine/datasets/catalog/JRC_GHSL_P2016_POP_GPW_GLOBE_V1#description
"""

GRID_SIZE = 5 * 1e3
"""
Split a city shape into a grid of elements with the specified size. Used for reverse-geocode all grid centroids.
"""

REF_WIKI_URL = 'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{language}.wikipedia.org/all-access/all-agents/{name}/{granularity}/{start}/{end}'
"""
Base url to query wikipedia pageviews
"""

OSM_TAGS = ['town', 'city']
OSM_ADMIN_LEVELS = [6, 7, 8]
OSM_TIMEOUT = 900
"""
Parameters for the query of OpenStreetMap data
"""

DATASET_NAME = os.getenv('DATASET_NAME')
TABLE_NAME = os.getenv('TABLE_NAME')
"""
BigQuery data-set and table to store processed shapes
"""
