import React, { useState, useEffect } from 'react';
import { Bar, Line } from 'react-chartjs-2';
import { Chart, registerables } from 'chart.js';
Chart.register(...registerables);

const API_BASE = "http://localhost:5001"; // Use your backend server URL and port

const CongestionStats = ({ selectedDate, selectedBorough }) => {
  const [topZones, setTopZones] = useState([]);
  const [hourlyVolume, setHourlyVolume] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setError(null);
        
        // Validate selectedDate
        if (!selectedDate || !/^\d{4}-\d{2}-\d{2}$/.test(selectedDate)) {
          throw new Error('Invalid date format. Use YYYY-MM-DD.');
        }

        // Fetch top congested zones
        const zonesRes = await fetch(
          `${API_BASE}/api/top-congested-zones?date=${selectedDate}&borough=${selectedBorough || 'all'}`
        );
        if (!zonesRes.ok) throw new Error('Failed to fetch zones');
        const zonesData = await zonesRes.json();
        
        // Fetch hourly traffic
        const hourlyRes = await fetch(
          `${API_BASE}/api/hourly-traffic?date=${selectedDate}&borough=${selectedBorough || 'all'}`
        );
        if (!hourlyRes.ok) throw new Error('Failed to fetch hourly data');
        const hourlyData = await hourlyRes.json();

        setTopZones(zonesData);
        setHourlyVolume(hourlyData);
      } catch (err) {
        console.error("Fetch error:", err);
        setError(err.message);
        setTopZones([]);
        setHourlyVolume(null);
      }
    };

    fetchData();
  }, [selectedDate, selectedBorough]);

  // Error state
  if (error) {
    return (
      <div className="congestion-stats error">
        <h2>Error Loading Data</h2>
        <p>{error}</p>
        <p>Please try selecting a different date or check the console for details.</p>
      </div>
    );
  }

  // Loading state
  if (!hourlyVolume || topZones.length === 0) {
    return (
      <div className="congestion-stats loading">
        <h2>Loading Traffic Insights...</h2>
        <p>Analyzing data for {selectedDate}</p>
      </div>
    );
  }

  // Chart data configurations
  const topZonesChart = {
    labels: topZones.map(zone => zone.zone_name),
    datasets: [{
      label: 'Average Speed (mph)',
      data: topZones.map(zone => zone.avg_speed_mph),
      backgroundColor: topZones.map(zone => 
        zone.avg_speed_mph < 7 ? 'rgba(255, 99, 132, 0.7)' : 
        zone.avg_speed_mph < 15 ? 'rgba(255, 205, 86, 0.7)' : 
        'rgba(75, 192, 192, 0.7)'
      ),
      borderColor: 'rgb(54, 162, 235)',
      borderWidth: 1
    }]
  };

  const hourlyChart = {
    labels: hourlyVolume.hours,
    datasets: [{
      label: 'Trips per Hour',
      data: hourlyVolume.counts,
      fill: false,
      borderColor: 'rgb(75, 192, 192)',
      tension: 0.1
    }]
  };

  return (
    <div className="congestion-stats">
      <h2>Traffic Insights for {selectedDate}</h2>
      
      <div className="stat-card">
        <h3>Top 5 Congested Zones</h3>
        <Bar 
          data={topZonesChart}
          options={{
            indexAxis: 'y',
            responsive: true,
            plugins: {
              legend: { display: false },
              tooltip: {
                callbacks: {
                  title: ([context]) => context.label,
                  label: (context) => [
                    `Speed: ${context.parsed.x} mph`,
                    `Trips: ${topZones[context.dataIndex].trip_count}`,
                    `Borough: ${topZones[context.dataIndex].borough}`
                  ]
                }
              }
            },
            scales: {
              x: { 
                title: { 
                  display: true, 
                  text: 'Average Speed (mph)',
                  font: { weight: 'bold' }
                }
              }
            }
          }}
        />
      </div>

      <div className="stat-card">
        <h3>Hourly Traffic Pattern</h3>
        <Line 
          data={hourlyChart}
          options={{
            plugins: {
              tooltip: {
                callbacks: {
                  label: (context) => `${context.dataset.label}: ${context.parsed.y}`
                }
              }
            },
            scales: {
              y: {
                title: {
                  display: true,
                  text: 'Number of Trips',
                  font: { weight: 'bold' }
                }
              }
            }
          }}
        />
      </div>

      <div className="insights-summary">
        <h3>Key Findings</h3>
        <ul>
          <li>
            🕒 Peak Congestion: {hourlyVolume.peak_hour}
          </li>
          <li>
            🚦 Worst Zone: {topZones[0]?.zone_name} ({topZones[0]?.avg_speed_mph} mph)
          </li>
          <li>
            🚕 Total Trips: {hourlyVolume.counts.reduce((a, b) => a + b, 0).toLocaleString()}
          </li>
          <li>
            🚙 Average Speed: {hourlyVolume.avg_speed} mph
          </li>
        </ul>
      </div>
    </div>
  );
};

export default CongestionStats;
