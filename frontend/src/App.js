import './App.css';
import RouteDashboard from './components/RouteDashboard';
import StopsDashboard from './components/StopsDashboard';
import AlertsSummary from './components/AlertsSummary';

function App() {
  return (
    <div className="App">
      <header className="App-header">
        Live Translink Analytics
      </header>
      <RouteDashboard />
      <StopsDashboard />
      <AlertsSummary />
    </div>
  );
}

export default App;
