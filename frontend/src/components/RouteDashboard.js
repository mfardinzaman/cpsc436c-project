import React, { useEffect, useState } from "react";
import mockRoutes from '../mock/mockRoutes.json';
import mockBuses from '../mock/mockVehicles.json'
import {
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableHead,
    TableRow,
    IconButton,
    Collapse,
    Typography,
    Box,
    Paper,
    Card,
} from "@mui/material";
import { KeyboardArrowDown, KeyboardArrowUp } from "@mui/icons-material";

const RouteDashboard = () => {
    const [routes, setRoutes] = useState(mockRoutes);

    return (
        <TableContainer component={Card} style={{
            width: '80%',
            margin: '20px auto',
            padding: '10px'
          }}>
            <Typography variant='h4' align='left'>Routes</Typography>
            <Table>
                <TableHead>
                    <TableRow>
                        <TableCell />
                        <TableCell>Route Name</TableCell>
                        <TableCell align="center">Number of Buses</TableCell>
                        <TableCell align="center">Average Lateness (min)</TableCell>
                        <TableCell align="center">% 5 Min Late</TableCell>
                    </TableRow>
                </TableHead>
                <TableBody>
                    {routes.map((route) => (
                        <React.Fragment key={route.routeId}>
                            {/* Main Row */}
                            <Row route={route} />
                        </React.Fragment>
                    ))}
                </TableBody>
            </Table>
        </TableContainer>
    );
};

const Row = ({ route }) => {
    const [open, setOpen] = useState(false)
    const [buses, setBuses] = useState(mockBuses)
    const toggleExpand = () => {
        setOpen(!open)
    }

    return (
        <>
            <TableRow>
                <TableCell>
                    <IconButton
                        onClick={toggleExpand}
                        aria-label="expand row"
                        size="small"
                    >
                        {open ? (
                            <KeyboardArrowUp />
                        ) : (
                            <KeyboardArrowDown />
                        )}
                    </IconButton>
                </TableCell>
                <TableCell>{`${route.route_short_name} ${route.route_long_name}`}</TableCell>
                <TableCell align="center">...</TableCell>
                <TableCell align="center">...</TableCell>
                <TableCell align="center">...</TableCell>
            </TableRow>
            {open &&
                <TableRow>
                    <TableCell colSpan={5}>
                        <Collapse in={open} timeout="auto" unmountOnExit>
                            <Box margin={2}>
                                <Typography variant="h6">
                                    Buses on {route.route_short_name}
                                </Typography>
                                <Table size="small">
                                    <TableHead>
                                        <TableRow>
                                            <TableCell>Label</TableCell>
                                            <TableCell>Status</TableCell>
                                            <TableCell>Lateness (min)</TableCell>
                                            <TableCell>Last Stop</TableCell>
                                            <TableCell>Last Update</TableCell>
                                        </TableRow>
                                    </TableHead>
                                    <TableBody>
                                        {buses.map((bus) => (
                                            <TableRow key={bus.vehicle_id}>
                                                <TableCell>{bus.vehicle_id}</TableCell>
                                                <TableCell>...</TableCell>
                                                <TableCell>...</TableCell>
                                                <TableCell>{bus.update_time}</TableCell>
                                                <TableCell>
                                                    {new Date(bus.update_time).toLocaleString()}
                                                </TableCell>
                                            </TableRow>
                                        ))}
                                    </TableBody>
                                </Table>
                            </Box>
                        </Collapse>
                    </TableCell>
                </TableRow>
            }
        </>
    )
}

export default RouteDashboard;
