import React, { useState, useCallback, useEffect } from "react";
import MUIDataTable from "mui-datatables";
import moment from 'moment-timezone';
import { TableRow, TableCell, Box, CircularProgress } from "@mui/material";
import service from "../services/services";

import DelayLineChart from "./charts/DelayLineChart";
import VeryLateLineChart from "./charts/VeryLateLineChart";
import VeryEarlyLineChart from "./charts/VeryEarlyLineChart";
import VehicleCountLineChart from "./charts/VehicleCountLineChart";
import { convertUnixTimeToPST } from "../utils/time";

const RouteExpandableRow = ({ rowData, route }) => {
    const [vehicles, setVehicles] = useState([])
    const [vehiclesLoading, setVehiclesLoading] = useState(false)
    const [historicalDataLoading, setHistoricalDataLoading] = useState(false);
    const [historicalData, setHistoricalData] = useState([])
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
        setVehiclesLoading(true)
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
            setVehiclesLoading(false);
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
                {
                    vehiclesLoading ? <CircularProgress /> : (
                        <MUIDataTable
                            title={"Vehicle Details"}
                            data={vehicles.map((bus) => ({
                                label: bus.vehicle_label,
                                lateness: Math.round(bus.delay / 60),
                                nextStop: bus.stop_name,
                                lastUpdate: convertUnixTimeToPST(moment.utc(bus.update_time).valueOf()),
                                expectedArrival: convertUnixTimeToPST(moment.utc(bus.expected_arrival).valueOf()),
                            }))}
                            columns={[
                                { name: "label", label: "Bus Label" },
                                { name: "lateness", label: "Lateness (min)" },
                                { name: "nextStop", label: "Next Stop" },
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

                        <DelayLineChart data={historicalData} />
                        <VehicleCountLineChart data={historicalData} />
                        <VeryLateLineChart data={historicalData} />
                        <VeryEarlyLineChart data={historicalData} />
                    </Box>
                )}

            </TableCell>
        </TableRow>
    );
};

export default RouteExpandableRow