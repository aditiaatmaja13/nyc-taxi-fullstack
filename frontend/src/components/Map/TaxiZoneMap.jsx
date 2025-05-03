import React, { useEffect, useState, useRef } from "react";
import { MapContainer, TileLayer, GeoJSON } from "react-leaflet";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import "leaflet/dist/leaflet.css";
import taxiZones from "../../data/taxi_zones.json";
import proj4 from "proj4";
import TaxiMarkers from "./TaxiMarkers";

// NYC State Plane (EPSG:2263) to WGS84
proj4.defs("EPSG:2263", "+proj=lcc +lat_1=41.03333333333333 +lat_2=40.66666666666666 +lat_0=40.16666666666666 +lon_0=-74 +x_0=300000.0000000001 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=us-ft +no_defs");

function transformCoords(coords) {
  if (typeof coords[0][0][0] === "number") {
    return coords.map(ring =>
      ring.map(coord => proj4("EPSG:2263", "WGS84", coord))
    );
  } else {
    return coords.map(poly =>
      poly.map(ring =>
        ring.map(coord => proj4("EPSG:2263", "WGS84", coord))
      )
    );
  }
}

function getColor(avgSpeed) {
  if (avgSpeed < 7) return "#FF0000";
  if (avgSpeed < 15) return "#FFD600";
  return "#00C853";
}

// Legend component
const Legend = () => {
  return (
    <div className="legend" style={{
      position: 'absolute', 
      bottom: '30px', 
      right: '10px', 
      zIndex: 1000, 
      background: 'white', 
      padding: '10px', 
      borderRadius: '5px',
      boxShadow: '0 1px 5px rgba(0,0,0,0.4)'
    }}>
      <h4>Traffic Conditions</h4>
      <div><span style={{background: '#FF0000', width: '20px', height: '20px', display: 'inline-block', marginRight: '5px'}}></span> Heavy (&lt;7 mph)</div>
      <div><span style={{background: '#FFD600', width: '20px', height: '20px', display: 'inline-block', marginRight: '5px'}}></span> Moderate (7-15 mph)</div>
      <div><span style={{background: '#00C853', width: '20px', height: '20px', display: 'inline-block', marginRight: '5px'}}></span> Smooth (&gt;15 mph)</div>
      <div><span style={{background: '#CCCCCC', width: '20px', height: '20px', display: 'inline-block', marginRight: '5px'}}></span> No data</div>
    </div>
  );
};

// Data context component
const DataContext = ({ dataTime, isHistorical }) => {
  return (
    <div style={{
      position: 'absolute',
      top: '10px',
      left: '10px',
      zIndex: 1000,
      background: 'white',
      padding: '10px',
      borderRadius: '5px',
      boxShadow: '0 1px 5px rgba(0,0,0,0.4)'
    }}>
      <h4>Data Context</h4>
      <div>{isHistorical ? "Historical Data Window" : "Current Data Window"}: {dataTime || "N/A"}</div>
      <div>Data reflects 15-minute intervals of taxi trip speeds</div>
    </div>
  );
};

const TaxiZoneMap = () => {
  const [congestion, setCongestion] = useState({});
  const [transformed, setTransformed] = useState(null);
  const [taxiData, setTaxiData] = useState([]);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [loading, setLoading] = useState(false);
  const [dataTimestamp, setDataTimestamp] = useState(null);
  const [isHistorical, setIsHistorical] = useState(false);
  const [startTime, setStartTime] = useState(null);
  const [endTime, setEndTime] = useState(null);
  const geoJsonRef = useRef(null);

  // Transform GeoJSON
  useEffect(() => {
    const transformedGeoJSON = {
      ...taxiZones,
      features: taxiZones.features.map(feature => ({
        ...feature,
        geometry: {
          ...feature.geometry,
          coordinates: transformCoords(feature.geometry.coordinates)
        }
      }))
    };
    setTransformed(transformedGeoJSON);
  }, []);

  // Fetch congestion data
  const fetchCongestion = async (start, end) => {
    setLoading(true);
    try {
      let url = "http://localhost:5001/api/congestion";
      if (isHistorical && start && end) {
        url = `http://localhost:5001/api/congestion/historical?start=${start.toISOString()}&end=${end.toISOString()}`;
      }

      const res = await fetch(url);
      const data = await res.json();
      
      const zoneData = data.zones || data;
      const timestamp = data.timestamp ? new Date(data.timestamp) : null;

      const mapping = {};
      if (Array.isArray(zoneData)) {
        zoneData.forEach(item => {
          const zoneId = item.pickup_zone || item._id;
          if (zoneId) {
            mapping[zoneId] = item.avg_speed_mph;
          }
        });
      }

      setCongestion(mapping);
      setDataTimestamp(timestamp ? timestamp.toLocaleString() : "Custom time range");
      setLastUpdate(new Date().toLocaleTimeString());
    } catch (err) {
      console.error("Failed to fetch congestion data:", err);
    } finally {
      setLoading(false);
    }
  };

  // Handle real-time/historical toggle
  useEffect(() => {
    if (isHistorical) {
      if (startTime && endTime) {
        fetchCongestion(startTime, endTime);
      }
    } else {
      const interval = setInterval(() => fetchCongestion(), 10000);
      fetchCongestion(); // Initial fetch
      return () => clearInterval(interval);
    }
  }, [isHistorical, startTime, endTime]);

  // Fetch taxi locations
  useEffect(() => {
    const fetchTaxis = async () => {
      try {
        const res = await fetch("http://localhost:5001/api/taxis");
        const data = await res.json();
        setTaxiData(data);
      } catch (err) {
        console.error("Failed to fetch taxi data:", err);
      }
    };
    fetchTaxis();
    const taxiInterval = setInterval(fetchTaxis, 5000);
    return () => clearInterval(taxiInterval);
  }, []);

  // Update styles when congestion data changes
  useEffect(() => {
    if (geoJsonRef.current && Object.keys(congestion).length > 0) {
      geoJsonRef.current.setStyle(style);
    }
  }, [congestion]);

  const style = (feature) => {
    const locId = feature.properties.LocationID;
    const hasData = congestion[locId] !== undefined;
    if (!hasData) {
      return {
        fillColor: "#CCCCCC",
        weight: 1,
        opacity: 1,
        color: "#333",
        fillOpacity: 0.3,
      };
    }
    return {
      fillColor: getColor(congestion[locId]),
      weight: 1,
      opacity: 1,
      color: "#333",
      fillOpacity: 0.5,
    };
  };

  const onEachFeature = (feature, layer) => {
    const locId = feature.properties.LocationID;
    const avgSpeed = congestion[locId]?.toFixed(2) || "No data";
    layer.bindTooltip(
      `<strong>${feature.properties.zone}</strong><br/>
       <strong>Borough: ${feature.properties.borough}</strong><br/>
       Zone ID: ${locId}<br/>
       ${avgSpeed !== "No data" ? `Avg Speed: ${avgSpeed} mph` : ''}`,
      { sticky: true }
    );
  };

  return (
    <div style={{ position: "relative" }}>
      <MapContainer
        center={[40.73, -73.93]}
        zoom={11}
        style={{ height: "80vh", width: "100%" }}
        scrollWheelZoom={true}
      >
        <TileLayer
          attribution='&copy; <a href="https://osm.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {transformed && (
          <GeoJSON 
            ref={geoJsonRef}
            key={`congestion-${Object.keys(congestion).length}-${lastUpdate}`}
            data={transformed}
            style={style}
            onEachFeature={onEachFeature}
          />
        )}
        <TaxiMarkers taxis={taxiData} />
      </MapContainer>
      
      {/* Legend */}
      <Legend />
      
      {/* Data Context */}
      <DataContext dataTime={dataTimestamp} isHistorical={isHistorical} />
      
      {/* Time Controls */}
      <div style={{
        position: 'absolute',
        top: '10px',
        right: '10px',
        zIndex: 1000,
        background: 'rgba(255,255,255,0.9)',
        padding: '8px',
        borderRadius: '4px',
        boxShadow: '0 0 5px rgba(0,0,0,0.2)',
        display: 'flex',
        flexDirection: 'column',
        gap: '8px'
      }}>
        <button 
          onClick={() => setIsHistorical(!isHistorical)}
          style={{ padding: '5px 10px' }}
        >
          {isHistorical ? "Switch to Live View" : "Switch to Historical View"}
        </button>

        {isHistorical && (
          <>
            <DatePicker
              selected={startTime}
              onChange={date => setStartTime(date)}
              selectsStart
              startDate={startTime}
              endDate={endTime}
              placeholderText="Start time"
              showTimeSelect
              dateFormat="Pp"
            />
            <DatePicker
              selected={endTime}
              onChange={date => setEndTime(date)}
              selectsEnd
              startDate={startTime}
              endDate={endTime}
              minDate={startTime}
              placeholderText="End time"
              showTimeSelect
              dateFormat="Pp"
            />
          </>
        )}

        {loading ? <span>Updating...</span> : 
         lastUpdate ? <span>Last updated: {lastUpdate}</span> : 
         <span>Waiting for data...</span>}
      </div>
    </div>
  );
};

export default TaxiZoneMap;
