import React, { useState } from "react";
import './App.css';
import RouteDashboard from './components/RouteDashboard';
import StopsDashboard from './components/StopsDashboard';
import AlertsSummary from './components/AlertsSummary';
import { ThemeProvider, createTheme, Tabs, Tab, Box } from '@mui/material';

const theme = createTheme({
  palette: {
    primary: {
      main: "#00355F",
    },
    background: {
      default: "#f5f5f5",
      paper: "#ffffff",
    },
    text: {
      primary: "#333333",
      secondary: "#666666",
    },
  },
  typography: {
    fontFamily: "'Inter', sans-serif",
    h1: {
      fontSize: "2.25rem",
      fontWeight: 700,
    },
    h2: {
      fontSize: "1.75rem",
      fontWeight: 600,
    },
    body1: {
      fontSize: "1rem",
    },
  },
  components: {
    MUIDataTableHeadCell: {
      styleOverrides: {
        contentWrapper: {
          justifyContent: 'center'
        },
      },
    },
    MuiButtonBase: {
      styleOverrides: {
        root: {
          padding: 0,
        },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          textTransform: "none",
          borderRadius: "8px",
          padding: "10px 20px",
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          boxShadow: "0 4px 8px rgba(0, 0, 0, 0.1)",
          borderRadius: "8px",
        },
      },
    },
    MuiTabs: {
      styleOverrides: {
        root: {
          minHeight: 35,
        },
      },
    },
    MuiTab: {
      styleOverrides: {
        root: {
          borderRadius: '8px',
          padding: '3px 5px',
          textTransform: 'none',
          minWidth: 80,
          minHeight: 35,
          marginRight: 16,
          transition: 'background-color 0.3s ease',
          '&:hover': {
            backgroundColor: '#f0f0f0',
          },
          '&.MuiTab-root': {
            '&:focus': {
              outline: 'none',
            },
          },
        },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        root: {
          textAlign: 'center',
          verticalAlign: 'middle',
        },
      },
    },
  },
});

function App() {
  const [tabIndex, setTabIndex] = useState(0);

  const handleTabChange = (_, newTabIndex) => {
    setTabIndex(newTabIndex);
  };

  return (
    <div className="App">
      <ThemeProvider theme={theme}>
        <header className="App-header">
          Live Translink Analytics
        </header>
        <Box sx={{ width: '100%' }}>
          <Tabs
            value={tabIndex}
            onChange={handleTabChange}
            variant="scrollable"
            scrollButtons="auto"
          >
            <Tab label="Routes" disableRipple/>
            <Tab label="Stops" disableRipple/>
            <Tab label="Alerts" disableRipple/>
          </Tabs>
        </Box>
        <Box sx={{ padding: 2 }}>
          {tabIndex === 0 && <RouteDashboard />}
          {tabIndex === 1 && <StopsDashboard />}
          {tabIndex === 2 && <AlertsSummary />}
        </Box>
      </ThemeProvider>
    </div>
  );
}

export default App;
