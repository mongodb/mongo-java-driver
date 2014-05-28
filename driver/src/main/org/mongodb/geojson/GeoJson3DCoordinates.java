package org.mongodb.geojson;

/**
 * Created by tgrall on 5/13/14.
 */
public class GeoJson3DCoordinates extends GeoJsonCoordinates {

    protected Double altitude = null;

    public GeoJson3DCoordinates(Double longitude, Double latitude, Double altitude) {
        super( longitude, latitude );
        this.altitude = altitude;
        coordinatesAsList.add(altitude);
    }

    public Double getAltitude(){
        return this.altitude;
    }

}
