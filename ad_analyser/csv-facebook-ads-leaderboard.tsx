import React, { useState, useEffect } from 'react';
import { Card, CardHeader, CardContent, CardFooter } from '@/components/ui/card';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';
import Papa from 'papaparse';

const FacebookAd = ({ ad }) => (
  <div className="facebook-ad bg-white rounded-lg shadow-md overflow-hidden" style={{ maxWidth: '500px' }}>
    <div className="p-4">
      <div className="flex items-center mb-2">
        <div className="w-10 h-10 bg-blue-600 rounded-full mr-2"></div>
        <div>
          <p className="font-bold">Your Page Name</p>
          <p className="text-xs text-gray-500">Sponsored · <span className="text-blue-500">Learn More</span></p>
        </div>
      </div>
      <p className="mb-2">{ad.body}</p>
    </div>
    <img 
      src={ad.image_path || `/api/placeholder/1080/1080`} 
      alt="Ad" 
      className="w-full aspect-square object-cover" 
    />
    <div className="p-4">
      <h3 className="font-bold text-xl mb-2">{ad.title}</h3>
      <p className="text-gray-500 mb-2">LIBREXIAAFSTUDY.COM</p>
      <button className="w-full bg-blue-600 text-white py-2 rounded font-bold">Learn More</button>
    </div>
  </div>
);

const TopImages = ({ ads }) => (
  <div className="mb-8">
    <h3 className="text-xl font-bold mb-4">Top 3 Images</h3>
    <div className="grid grid-cols-3 gap-4">
      {ads.slice(0, 3).map((ad, index) => (
        <img 
          key={index}
          src={ad.image_path || `/api/placeholder/1080/1080`} 
          alt={`Top image ${index + 1}`} 
          className="w-full aspect-square object-cover rounded-lg"
        />
      ))}
    </div>
  </div>
);

const AdsLeaderboard = ({ ads }) => {
  const sortedAds = [...ads].sort((a, b) => b['Referrals B/AA'] - a['Referrals B/AA']);

  const chartData = sortedAds.map(ad => ({
    name: ad.title.slice(0, 20) + '...',
    referrals: ad['Referrals B/AA']
  }));

  return (
    <div className="p-4">
      <h2 className="text-2xl font-bold mb-4">Ads Leaderboard</h2>
      <TopImages ads={sortedAds} />
      <div className="mb-8">
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chartData}>
            <XAxis dataKey="name" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="referrals" fill="#8884d8" />
          </BarChart>
        </ResponsiveContainer>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
        {sortedAds.map((ad, index) => (
          <div key={index} className="flex flex-col">
            <FacebookAd ad={ad} />
            <div className="mt-4 text-sm">
              <p>Referrals B/AA: {ad['Referrals B/AA']}</p>
              <p>Sessions B/AA: {ad['Sessions B/AA']}</p>
              <p>Referrals: {ad.Referrals}</p>
              <p>Sessions: {ad.Sessions}</p>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

const CSVFileInput = ({ onDataLoaded }) => {
  const handleFileUpload = (event) => {
    const file = event.target.files[0];
    Papa.parse(file, {
      complete: (result) => {
        const parsedData = result.data.slice(1).map(row => ({
          title: row[0],
          body: row[1],
          image_path: row[2],
          Referrals: parseFloat(row[3]),
          'Referrals B/AA': parseFloat(row[4]),
          Sessions: parseFloat(row[5]),
          'Sessions B/AA': parseFloat(row[6]),
          clicks: parseFloat(row[7]),
          impressions: parseFloat(row[8]),
          reach: parseFloat(row[9])
        }));
        onDataLoaded(parsedData);
      },
      header: true,
      skipEmptyLines: true
    });
  };

  return (
    <div className="mb-4">
      <input 
        type="file" 
        accept=".csv" 
        onChange={handleFileUpload} 
        className="block w-full text-sm text-gray-500
          file:mr-4 file:py-2 file:px-4
          file:rounded-full file:border-0
          file:text-sm file:font-semibold
          file:bg-blue-50 file:text-blue-700
          hover:file:bg-blue-100"
      />
    </div>
  );
};

const App = () => {
  const [adsData, setAdsData] = useState([]);

  const handleDataLoaded = (data) => {
    setAdsData(data);
  };

  return (
    <div className="container mx-auto">
      <CSVFileInput onDataLoaded={handleDataLoaded} />
      {adsData.length > 0 && <AdsLeaderboard ads={adsData} />}
    </div>
  );
};

export default App;
