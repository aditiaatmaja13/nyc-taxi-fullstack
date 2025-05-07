import React, { useState } from 'react';
import TaxiZoneMap from './components/Map/TaxiZoneMap';
import CongestionStats from './components/Dashboard/CongestionStats';

function App() {
  // Default to first day of your Kafka data (2025-01-01)
  const [selectedDate, setSelectedDate] = useState('2025-01-01');

  return (
    <div className="App">
      <h1>NYC Taxi Zone Congestion Map</h1>
      
      {/* Date picker */}
      <div style={{ margin: '20px' }}>
        <label>Select Date: </label>
        <input
          type="date"
          value={selectedDate}
          min="2025-01-01"
          max="2025-01-31"  
          onChange={(e) => setSelectedDate(e.target.value)}
        />
      </div>

      <TaxiZoneMap selectedDate={selectedDate} />
      <CongestionStats selectedDate={selectedDate} />
    </div>
  );
}

export default App;
