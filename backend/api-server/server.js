const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');

const app = express();
app.use(cors());

const uri = 'mongodb://localhost:27017';
const client = new MongoClient(uri);

// Helper function to connect to MongoDB
async function connectDB() {
  await client.connect();
  return client.db('nyc_congestion');
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

// Get taxi locations
async function getTaxiLocations() {
  const db = await connectDB();
  const collection = db.collection('taxi_locations');

  const pipeline = [
    { $sort: { timestamp: -1 } },
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

app.get('/api/taxis', async (req, res) => {
  try {
    const data = await getTaxiLocations();
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.toString() });
  }
});

const PORT = 5001;
app.listen(PORT, () => console.log(`API server running on port ${PORT}`));
