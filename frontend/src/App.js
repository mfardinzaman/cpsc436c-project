import logo from './logo.svg';
import './App.css';
import RouteDashboard from './components/RouteDashboard';
import { Typography } from '@mui/material';

function App() {
  return (
    <div className="App">
      <header className="App-header">
        Live Translink Analytics
      </header>
      <RouteDashboard />
    </div>
  );
}

export default App;
