import React, { useState, useCallback, useEffect } from "react";
import MUIDataTable from "mui-datatables";
import moment from 'moment-timezone';
import { TableRow, TableCell, Box, CircularProgress } from "@mui/material";
import service from "../services/services";

import RouteDelayLineChart from "./charts/RouteDelayLineChart";
import RouteVeryLateLineChart from "./charts/RouteVeryLateLineChart";
import RouteVeryEarlyLineChart from "./charts/RouteVeryEarlyLineChart";
import RouteVehicleCountLineChart from "./charts/RouteVehicleCountLineChart";
import { convertUnixTimeToPST } from "../utils/time";

const RouteExpandableRow = ({ rowData, rowMeta, routes }) => {
    const [vehicles, setVehicles] = useState([])
    const [vehiclesLoading, setVehiclesLoading] = useState(false)
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
    }, [routeData])

    const getVehiclesData = useCallback(async () => {
        setVehiclesLoading(true)
        try {
            const result = await service.getRouteVehicles({
                route_id: routeData['route_id'],
                direction_id: routeData['direction_id']
            });

            if (result.statusCode === 200) {
                setVehicles(result.body)
            } else {
                console.log('Error fetching route stats over time:', result);
            }
        } catch (error) {
            console.log('Error:', error);
        } finally {
            setVehiclesLoading(false);
        }
    }, [routeData])

    useEffect(() => {
        getHistoricalData();
    }, [getHistoricalData])

    useEffect(() => {
        getVehiclesData();
    }, [getVehiclesData])

    return (
        <TableRow
            sx={{
                padding: 2,
            }}
        >
            <TableCell colSpan={colSpan}>
                {
                    vehiclesLoading ? <CircularProgress /> : (
                        <MUIDataTable
                            title={"Vehicle Details"}
                            data={vehicles.map((bus) => ({
                                label: bus.vehicle_label,
                                lateness: Math.round(bus.delay / 60),
                                lastStop: bus.stop_id,
                                lastUpdate: convertUnixTimeToPST(moment.utc(bus.update_time).valueOf()),
                                expectedArrival: convertUnixTimeToPST(moment.utc(bus.expected_arrival).valueOf()),
                            }))}
                            columns={[
                                { name: "label", label: "Bus Label" },
                                { name: "lateness", label: "Lateness (min)" },
                                { name: "lastStop", label: "Last Stop" },
                                { name: "expectedArrival", label: "Expected Arrival Time" },
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
                    )
                }
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