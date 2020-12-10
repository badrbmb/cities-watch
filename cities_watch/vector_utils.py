import ee

from cities_watch import config


def vectorize_image(image, geometry, verbose=config.VERBOSE, urban_only=True, scale_multi=config.SCALE_MULTI):
    if verbose:
        print('Converting Raster to Vectors ...')
    # Vectoring prediction over specified geometry
    city_image = image.gte(config.CITY_THRESHOLD)
    if urban_only:
        # Mask all pixels not predicted as city
        city_image = city_image.updateMask(city_image.neq(0))

    city_vectors = city_image.addBands(image.select('classes')) \
        .reduceToVectors(geometry=geometry,
                         scale=scale_multi * config.SCALE,  # Try reduce the size of computations
                         geometryType='polygon',
                         reducer=ee.Reducer.mean(),
                         maxPixels=1e9,
                         tileScale=config.TILESCALE,
                         bestEffort=True,
                         eightConnected=True,
                         labelProperty='city')

    # re-assign id in properties
    city_vectors = city_vectors.map(lambda x: x.set({'id': ee.Feature(x).id()}))

    return city_vectors
