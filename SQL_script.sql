CREATE TABLE citibike_tripdata (
    tripduration INTEGER,
    starttime TIMESTAMP,
    stoptime TIMESTAMP,
    startstationid INTEGER,
    startstationname VARCHAR(255),
    startstationlatitude VARCHAR(255),
    startstationlongitude VARCHAR(255),
    endstationid INTEGER,
    endstationname VARCHAR(255),
    endstationlatitude VARCHAR(255),
    endstationlongitude VARCHAR(255),
    bikeid INTEGER,
    usertype VARCHAR(50),
    birthyear VARCHAR(10),
    gender INTEGER
)
PARTITION BY RANGE (starttime) (
    PARTITION p2015_09 START ('2015-09-01') END ('2015-10-01')
);
