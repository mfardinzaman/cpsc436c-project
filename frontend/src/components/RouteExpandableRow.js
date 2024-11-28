import React, { useState, useCallback, useEffect } from "react";
import MUIDataTable from "mui-datatables";
import moment from 'moment-timezone';
import { TableRow, TableCell, Box, CircularProgress } from "@mui/material";
import service from "../services/services";

import mockBuses from "../mock/mockVehicles.json";
import RouteDelayLineChart from "./charts/RouteDelayLineChart";
import RouteVeryLateLineChart from "./charts/RouteVeryLateLineChart";
import RouteVeryEarlyLineChart from "./charts/RouteVeryEarlyLineChart";
import RouteVehicleCountLineChart from "./charts/RouteVehicleCountLineChart";

const RouteExpandableRow = ({ rowData, rowMeta, routes }) => {
    const [buses, setBuses] = useState(mockBuses)
    const [historicalDataLoading, setHistoricalDataLoading] = useState(false);
    const [historicalData, setHistoricalData] = useState([])
    const colSpan = rowData.length + 1;
    const routeData = routes[rowMeta.rowIndex]

    const getHistoricalData = useCallback(async () => {
        setHistoricalDataLoading(true)
        try {
            const result = await service.getRouteStatsOverTime({
                route_id: routeData['route_id'],
                directionId: routeData['direction_id']
            });

            if (result.statusCode === 200) {
                setHistoricalData(result.body.map((data) => ({
                    update_time: moment.utc(data.update_time).valueOf(),
                    average_delay: data.average_delay / 60,
                    early_percentage: Math.floor((data.very_early_count / data.vehicle_count) * 100),
                    late_percentage: Math.floor((data.very_late_count / data.vehicle_count) * 100),
                    vehicle_count: data.vehicle_count
                })))
            } else {
                console.log('Error fetching route stats over time:', result);
            }
        } catch (error) {
            console.log('Error:', error);
        } finally {
            setHistoricalDataLoading(false);
        }
        console.log(historicalData)
    }, [routeData])

    useEffect(() => {
        getHistoricalData();
    }, [getHistoricalData])
    return (
        <TableRow
            sx={{
                padding: 2,
            }}
        >
            <TableCell colSpan={colSpan}>
                <MUIDataTable
                    title={"Buses Details"}
                    data={buses.map((bus) => ({
                        label: bus.vehicle_label,
                        status: bus.current_status,
                        lateness: Math.floor(Math.random() * 15), // mock
                        lastStop: `Stop ${Math.floor(Math.random() * 10)}`, // mock
                        lastUpdate: new Date(bus.update_time).toLocaleString(),
                    }))}
                    columns={[
                        { name: "label", label: "Bus Label" },
                        { name: "status", label: "Status" },
                        { name: "lateness", label: "Lateness (min)" },
                        { name: "lastStop", label: "Last Stop" },
                        { name: "lastUpdate", label: "Last Update" },
                    ]}
                    options={{
                        selectableRows: "none",
                        pagination: false,
                        search: false,
                        print: false,
                        download: false,
                        filter: false,
                        responsive: 'standard'
                    }}
                />
                {historicalDataLoading ? <CircularProgress /> : (
                    <Box
                        sx={{
                            display: 'grid',
                            gap: 2,
                            gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' },
                            padding: 2,
                        }}
                    >

                        <RouteDelayLineChart data={historicalData} />
                        <RouteVehicleCountLineChart data={historicalData} />
                        <RouteVeryLateLineChart data={historicalData} />
                        <RouteVeryEarlyLineChart data={historicalData} />
                    </Box>
                )}

            </TableCell>
        </TableRow>
    );
};

export default RouteExpandableRow