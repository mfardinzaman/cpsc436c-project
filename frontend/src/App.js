import logo from './logo.svg';
import './App.css';
import RouteDashboard from './components/RouteDashboard';
import { Typography } from '@mui/material';
import StopsDashboard from './components/StopsDashboard';

function App() {
  return (
    <div className="App">
      <header className="App-header">
        Live Translink Analytics
      </header>
      <RouteDashboard />
      <StopsDashboard />
    </div>
  );
}

export default App;
