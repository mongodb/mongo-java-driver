package org.mongodb.geojson;

@SuppressWarnings(value = {"unchecked", "rawtypes"})
public class GeoJson2DCoordinates extends GeoJsonCoordinates {
    private static final long serialVersionUID = -8211024611761204600L;

    public GeoJson2DCoordinates(final Double longitude, final Double latitude) {
        super(longitude, latitude);
    }

}
