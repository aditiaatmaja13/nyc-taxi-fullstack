import React from "react";
import { Marker, Tooltip } from "react-leaflet";
import L from "leaflet";
import taxiIconImg from "../../assets/icons/mini-yellow-taxi.png";

const taxiIcon = new L.Icon({
  iconUrl: taxiIconImg,
  iconSize: [32, 32],
  iconAnchor: [16, 16],
});

const TaxiMarkers = ({ taxis }) => (
  <>
    {taxis.map((taxi) => (
      <Marker
        key={taxi.id}
        position={[taxi.lat, taxi.lng]}
        icon={taxiIcon}
      >
        <Tooltip>Taxi ID: {taxi.id}</Tooltip>
      </Marker>
    ))}
  </>
);

export default TaxiMarkers;
