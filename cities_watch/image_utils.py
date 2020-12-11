from datetime import datetime

import ee
import numpy as np

from cities_watch import config


def mask2clouds(img):
    '''
    Function to mask clouds based on the pixel_qa band of Landsat 5 or 8 data. See:
    https://developers.google.com/earth-engine/datasets/catalog/LANDSAT_LT05_C01_T1_SR
    https://developers.google.com/earth-engine/datasets/catalog/LANDSAT_LC08_C01_T1_SR

    Params:
    -------
    - img: image input Landsat 5/8 SR image

    Return:
    -------
    cloudmasked Landsat 5/8 image
    '''

    qa = img.select('pixel_qa')
    cloud = qa.bitwiseAnd(1 << 5) \
        .And(qa.bitwiseAnd(1 << 7)) \
        .Or(qa.bitwiseAnd(1 << 3))
    mask2 = img.mask().reduce(ee.Reducer.min())
    return img.updateMask(cloud.Not()).updateMask(mask2)


def load_ms_image(start_date, end_date, geometry=None, selected_bands=True):

    ms_collection = ee.ImageCollection([])
    for ref_col, dict_cor in config.MS_BANDS_CORRESPONDENCE.items():
        raw_bands = list(dict_cor.keys())
        bands = list(dict_cor.values())

        tmp_collection = ee.ImageCollection(f"{ref_col}/{config.MS_LEVEL}") \
            .filterDate(start_date, end_date) \
            .filter(ee.Filter.lt('CLOUD_COVER', config.MAX_CLOUD_COVER)) \
            .map(mask2clouds) \
            .select(raw_bands, bands)  # rename bands

        ms_collection = ee.ImageCollection(ms_collection.merge(tmp_collection))

    if geometry:
        ms_collection = ms_collection.filterBounds(geometry)

    # Form median composite
    out_image = ms_collection.median().multiply(0.0001)

    ms_ndvi = out_image.normalizedDifference(config.NDVI_bands).rename(['NDVI'])

    if selected_bands:
        out_image = out_image.select(config.RGB_bands).rename(['R', 'G', 'B'])

    out_image = out_image.addBands(ms_ndvi)

    return out_image


def get_scaling_image_ols_dnb(common_range=config.COMMON_RANGE_NTL,
                              use_image=True,
                              control_shape=config.CONTROL_SHAPE):

    start, end = common_range
    dmsp_ols = load_dmsp_ols_collection(start_date=start, end_date=end, apply_scaling=False).median()
    bnd_viirs = load_bnd_viirs_collection(start_date=start, end_date=end).median()

    if use_image:
        scaling_image = bnd_viirs.divide(dmsp_ols)
    else:
        control_geom = ee.Geometry(control_shape)

        val_dmsp_ols = dmsp_ols.reduceRegion(reducer=ee.Reducer.mean(),
                                             geometry=control_geom,
                                             scale=config.SCALE,
                                             bestEffort=True).get('radiance')

        val_bnd_viirs = bnd_viirs.reduceRegion(reducer=ee.Reducer.mean(),
                                               geometry=control_geom,
                                               scale=config.SCALE,
                                               bestEffort=True).get('radiance')

        scaling_factor = ee.Number(val_bnd_viirs).divide(ee.Number(val_dmsp_ols))

        scaling_image = ee.Image(scaling_factor)

    return scaling_image


def load_dmsp_ols_collection(start_date, end_date, geometry=None, apply_scaling=True, use_image=True):

    # Load DMSP-OLS imagery
    dmsp_ols_collection = ee.ImageCollection('NOAA/DMSP-OLS/NIGHTTIME_LIGHTS') \
        .filterDate(start_date, end_date) \
        .select(['stable_lights'], ['radiance'])

    if geometry:
        dmsp_ols_collection = dmsp_ols_collection.filterBounds(geometry)

    if apply_scaling:
        # apply scaling to each image
        scaling_img = get_scaling_image_ols_dnb(use_image=use_image)
        dmsp_ols_collection = dmsp_ols_collection.map(lambda x: x.multiply(scaling_img))

    return dmsp_ols_collection


def load_bnd_viirs_collection(start_date, end_date, geometry=None):

    viirs_dnb_collection = ee.ImageCollection('NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG') \
        .filterDate(start_date, end_date) \
        .select(['avg_rad'], ['radiance'])

    if geometry:
        viirs_dnb_collection = viirs_dnb_collection.filterBounds(geometry)

    return viirs_dnb_collection


def load_nl_image(start_date, end_date, geometry=None, apply_scaling=True, use_image=True):

    # Load DMSP-OLS imagery
    dmsp_ols_collection = load_dmsp_ols_collection(start_date, end_date,
                                                   geometry=geometry,
                                                   apply_scaling=apply_scaling,
                                                   use_image=use_image)

    viirs_dnb_collection = load_bnd_viirs_collection(start_date, end_date,
                                                     geometry=geometry)

    # merge two collections
    nl_collection = dmsp_ols_collection.merge(viirs_dnb_collection)

    nl_image = nl_collection.median()

    return nl_image


def check_aoi_coverage(geometry, ref_collection, start_date=None, end_date=None, verbose=config.VERBOSE):
    if verbose:
        print(f"Coverage check v.s {ref_collection} ...")

    collection = ee.ImageCollection(ref_collection).filterBounds(geometry)
    if (start_date is not None) & (end_date is not None):
        collection = collection.filterDate(start_date, end_date)

    # Get the cumulative footprint of all the images in the collection
    collection_geom = collection.geometry().dissolve()

    corrected_geometry = geometry.intersection(collection_geom)

    if verbose:
        old_area = round(geometry.area().getInfo() / 1e6, 3)
        new_area = round(corrected_geometry.area().getInfo() / 1e6, 3)
        print(f'Requested coverage: {old_area}km2 --> Actual coverage: {new_area}km2')

    return corrected_geometry


def load_feature_stack(start_date, end_date, geometry=None, check_geometry=False):

    if geometry and check_geometry:
        # Make sure the geometry is contained inside the images footprint
        # Only checking for multi-spectral imagery (where the coverage excludes high seas)
        geometry = check_aoi_coverage(geometry, config.REF_MS_COLLECTION, start_date=start_date, end_date=end_date)

    optical_image = load_ms_image(start_date, end_date, geometry=geometry)

    radiance_image = load_nl_image(start_date, end_date, geometry=geometry)

    # form feature stack for inference
    feature_stack = ee.Image.cat([
        radiance_image.select('radiance'),
        optical_image
    ]).float()

    if geometry:
        feature_stack = feature_stack.clip(geometry)

    return feature_stack


def load_population_count(start_date, end_date, ref_collection='JRC/GHSL/P2016/POP_GPW_GLOBE_V1',
                          threshold=config.POP_THRESHOLD, geometry=None):
    def _get_closest_year(ref_date):
        available_years = [1975, 1990, 2000, 2015]
        # reassign start and end dates based on closest available year
        dt_times = [abs((datetime(t, 1, 1) - datetime.strptime(ref_date, '%Y-%m-%d')))
                    for t in available_years]
        closest_date = available_years[np.argmin(dt_times)]
        return closest_date

    start_date = f'{_get_closest_year(start_date)}-01-01'
    end_date = f'{_get_closest_year(end_date)}-12-31'

    # Get population count
    dataset = ee.ImageCollection(ref_collection) \
        .filter(ee.Filter.date(start_date, end_date))

    if geometry:
        dataset = dataset.filterBounds(geometry)

    # Get max population_count on period
    population_count = dataset.select('population_count').toBands()
    population_count = population_count.reduce(ee.Reducer.max())

    if threshold:
        # convert to binary based on threshold
        population_count = population_count.where(population_count.lt(threshold), 0) \
            .where(population_count.gte(threshold), 1)

    return population_count
