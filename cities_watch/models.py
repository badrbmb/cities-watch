import ee

from cities_watch import config
from cities_watch.image_utils import load_feature_stack
from cities_watch.vector_utils import vectorize_image


def load_model():
    model = ee.Model.fromAiPlatformPredictor(
        projectName=config.PROJECT_NAME,
        modelName=config.MODEL_NAME,
        version=config.MODEL_VERSION,
        inputTileSize=config.INPUT_TILE_SIZE,
        inputOverlapSize=config.INPUT_OVERLAP_SIZE,
        proj=ee.Projection('EPSG:4326').atScale(config.SCALE),
        fixInputProj=True,
        outputBands={config.RESPONSE: {
            'type': ee.PixelType.float()
        }
        }
    )
    return model


def map_urban_areas(aoi, start_date, end_date, model=None, vectorized=True, verbose=config.VERBOSE,
                    scale_multi=config.SCALE_MULTI):
    aoi_props = aoi['properties']

    if verbose:
        print(f"""
            Mapping cities for {aoi_props} 
            date range: {start_date}/{end_date} ...
            """)

    feature = ee.Feature(aoi)

    # Load feature stack with optical and radiance imagery
    feature_stack = load_feature_stack(start_date=start_date,
                                       end_date=end_date,
                                       geometry=feature.geometry())  # .buffer(config.BUFFER_COEFF)

    if not model:
        # Load the default trained model if not defined
        model = load_model()

    # Run inference
    predictions = model.predictImage(feature_stack.toArray()).clip(feature.geometry())

    out_images = {"features": feature_stack, 'predictions': predictions}

    if vectorized:
        # Convert prediction raster to vectors
        city_vectors = vectorize_image(image=predictions, geometry=feature.geometry(), scale_multi=scale_multi)
        return out_images, city_vectors

    else:
        return out_images
