import React, { useState, useCallback, useEffect } from "react";
import moment from 'moment-timezone';
import { TableRow, TableCell, Box, CircularProgress } from "@mui/material";
import service from "../services/services";

import DelayLineChart from "./charts/DelayLineChart";
import VeryLateLineChart from "./charts/VeryLateLineChart";
import VeryEarlyLineChart from "./charts/VeryEarlyLineChart";
import VehicleCountLineChart from "./charts/VehicleCountLineChart";

import RouteVehicleDetailsTable from "./tables/RouteVehicleDetailsTable";

const RouteExpandableRow = ({ rowData, route }) => {
    const [vehicles, setVehicles] = useState([])
    const [historicalData, setHistoricalData] = useState([])
    const [vehiclesDataLoading, setVehiclesDataLoading] = useState(false);
    const [historicalDataLoading, setHistoricalDataLoading] = useState(false);

    const colSpan = rowData.length + 1;

    const getHistoricalData = useCallback(async () => {
        setHistoricalDataLoading(true)
        try {
            const result = await service.getRouteStatsOverTime({
                route_id: route['route_id'],
                directionId: route['direction_id']
            });

            if (result.statusCode === 200) {
                setHistoricalData(result.body.map((data) => ({
                    update_time: moment.utc(data.update_time).valueOf(),
                    average_delay: data.average_delay / 60,
                    early_percentage: (data.very_early_count / data.vehicle_count) * 100,
                    late_percentage: (data.very_late_count / data.vehicle_count) * 100,
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
    }, [route])

    const getVehiclesData = useCallback(async () => {
        setVehiclesDataLoading(true)
        try {
            const result = await service.getRouteVehicles({
                route_id: route['route_id'],
                direction_id: route['direction_id']
            });

            if (result.statusCode === 200) {
                setVehicles(result.body)
            } else {
                console.log('Error fetching route stats over time:', result);
            }
        } catch (error) {
            console.log('Error:', error);
        } finally {
            setVehiclesDataLoading(false);
        }
    }, [route])

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
                {(vehiclesDataLoading || historicalDataLoading) ? <CircularProgress /> : (
                    <>
                        <Box
                            sx={{
                                display: 'grid',
                                gap: 2,
                                gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' },
                                padding: 2,
                            }}
                        >

                            <DelayLineChart data={historicalData} />
                            <VehicleCountLineChart data={historicalData} />
                            <VeryLateLineChart data={historicalData} />
                            <VeryEarlyLineChart data={historicalData} />
                        </Box>
                        {
                            route.route_id !== 'ALL' && (
                                <RouteVehicleDetailsTable vehicles={vehicles} />
                            )
                        }
                    </>
                )}
            </TableCell>
        </TableRow>
    );
};

export default RouteExpandableRow