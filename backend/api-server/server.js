const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');

const app = express();

// Enhanced CORS config for frontend on port 3001
app.use(cors({
  origin: 'http://localhost:3001',
  optionsSuccessStatus: 200
}));

// Security headers
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  next();
});

// Request logging
app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.url}`);
  next();
});

const uri = 'mongodb://localhost:27017';
const client = new MongoClient(uri);
let db; // Single connection reference

// Optimized DB connection
async function connectDB() {
  if (!db) {
    await client.connect();
    db = client.db('nyc_congestion');
  }
  return db;
}

// Get latest congestion data
async function getLatestCongestion() {
  const db = await connectDB();
  const collection = db.collection('traffic_congestion');

  const pipeline = [
    { $sort: { timestamp: -1 } },
    {
      $group: {
        _id: '$pickup_zone',
        avg_speed_mph: { $first: '$avg_speed_mph' },
        congestion_level: { $first: '$congestion_level' },
        timestamp: { $first: '$timestamp' },
        borough: { $first: '$borough' },
        zone_name: { $first: '$zone_name' },
        latest_overall: { $max: '$timestamp' }
      }
    }
  ];

  const results = await collection.aggregate(pipeline).toArray();
  
  return {
    data: results,
    timestamp: results.length > 0 ? results[0].latest_overall : null
  };
}

// Get historical congestion data
async function getHistoricalCongestion(start, end) {
  const db = await connectDB();
  const collection = db.collection('traffic_congestion');

  const pipeline = [
    {
      $match: {
        timestamp: {
          $gte: new Date(start),
          $lte: new Date(end)
        }
      }
    },
    {
      $group: {
        _id: '$pickup_zone',
        avg_speed_mph: { $avg: '$avg_speed_mph' },
        borough: { $first: '$borough' },
        zone_name: { $first: '$zone_name' },
        timestamp: { $max: '$timestamp' }
      }
    }
  ];

  const results = await collection.aggregate(pipeline).toArray();
  return {
    data: results,
    timestamp: end
  };
}

// Get taxi locations (configurable limit, default: all)
async function getTaxiLocations(limit) {
  const db = await connectDB();
  const collection = db.collection('taxi_locations');

  const pipeline = [
    { $sort: { timestamp: -1 } },
    ...(limit ? [{ $limit: limit }] : []),
    {
      $group: {
        _id: '$taxi_id',
        lat: { $first: '$lat' },
        lng: { $first: '$lng' },
        timestamp: { $first: '$timestamp' }
      }
    }
  ];

  const results = await collection.aggregate(pipeline).toArray();
  return results.map(taxi => ({
    id: taxi._id,
    lat: taxi.lat,
    lng: taxi.lng
  }));
}

// API Endpoints
app.get('/api/congestion', async (req, res) => {
  try {
    const { data, timestamp } = await getLatestCongestion();
    res.json({
      zones: data,
      timestamp
    });
  } catch (err) {
    res.status(500).json({ error: err.toString() });
  }
});

app.get('/api/congestion/historical', async (req, res) => {
  try {
    const { start, end } = req.query;
    
    if (!start || !end) {
      return res.status(400).json({ error: "Missing start or end parameters" });
    }

    const { data, timestamp } = await getHistoricalCongestion(start, end);
    res.json({
      zones: data,
      timestamp
    });
  } catch (err) {
    res.status(500).json({ error: err.toString() });
  }
});

// Configurable limit for /api/taxis
app.get('/api/taxis', async (req, res) => {
  try {
    const limit = req.query.limit ? parseInt(req.query.limit) : undefined;
    const data = await getTaxiLocations(limit);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.toString() });
  }
});

// Statistics Endpoints - UTC date handling
app.get('/api/top-congested-zones', async (req, res) => {
  try {
    const { date, borough } = req.query;
    const db = await connectDB();
    const collection = db.collection('traffic_congestion');

    const startDate = new Date(date + 'T00:00:00Z');
    const endDate = new Date(date + 'T23:59:59.999Z');

    const filter = { 
      timestamp: { 
        $gte: startDate,
        $lte: endDate
      }
    };
    
    if (borough && borough !== 'all') {
      filter.borough = borough;
    }

    const pipeline = [
      { $match: filter },
      { $group: {
          _id: '$pickup_zone',
          zone_name: { $first: '$zone_name' },
          borough: { $first: '$borough' },
          avg_speed_mph: { $avg: '$avg_speed_mph' },
          trip_count: { $sum: 1 }
      }},
      { $sort: { avg_speed_mph: 1 } },
      { $limit: 5 },
      { $project: {
          _id: 0,
          zone_id: '$_id',
          zone_name: 1,
          borough: 1,
          avg_speed_mph: { $round: ['$avg_speed_mph', 2] },
          trip_count: 1
      }}
    ];

    const results = await collection.aggregate(pipeline).toArray();
    res.json(results);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/hourly-traffic', async (req, res) => {
  try {
    const { date, borough } = req.query;
    const db = await connectDB();
    const collection = db.collection('traffic_congestion');

    const startDate = new Date(date + 'T00:00:00Z');
    const endDate = new Date(date + 'T23:59:59.999Z');

    const filter = { 
      timestamp: { 
        $gte: startDate,
        $lte: endDate
      }
    };
    
    if (borough && borough !== 'all') {
      filter.borough = borough;
    }

    const pipeline = [
      { $match: filter },
      { $project: {
          hour: { $hour: '$timestamp' },
          avg_speed_mph: 1
      }},
      { $group: {
          _id: '$hour',
          trip_count: { $sum: 1 },
          avg_speed: { $avg: '$avg_speed_mph' }
      }},
      { $sort: { _id: 1 } },
      { $project: {
          _id: 0,
          hour: '$_id',
          trip_count: 1,
          avg_speed: { $round: ['$avg_speed', 2] }
      }}
    ];

    const results = await collection.aggregate(pipeline).toArray();
    
    // Format for chart.js
    const hours = Array.from({length: 24}, (_,i) => `${i}:00`);
    const counts = Array(24).fill(0);
    const speeds = Array(24).fill(0);

    results.forEach(item => {
      counts[item.hour] = item.trip_count;
      speeds[item.hour] = item.avg_speed;
    });

    const peakHour = counts.indexOf(Math.max(...counts));
    
    res.json({
      hours: hours.map(h => `${h} - ${parseInt(h)+1}:00`),
      counts,
      speeds,
      peak_hour: `${peakHour}:00 - ${peakHour+1}:00`,
      avg_speed: (speeds.reduce((a,b) => a + b, 0) / 24).toFixed(2)
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Global error handler
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

const PORT = 5001;
app.listen(PORT, () => console.log(`API server running on port ${PORT}`));
