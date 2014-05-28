package org.mongodb.geojson;


@SuppressWarnings(value = {"unchecked", "rawtypes"})
public class GeoJson3DCoordinates extends GeoJsonCoordinates {
    private static final long serialVersionUID = -6038544500857815673L;

    private Double altitude = null;

    public GeoJson3DCoordinates(final Double longitude, final Double latitude, final Double altitude) {
        super(longitude, latitude);
        this.altitude = altitude;
        this.getCoordinatesAsList().add(altitude);
    }

    public Double getAltitude(){
        return this.altitude;
    }

}
