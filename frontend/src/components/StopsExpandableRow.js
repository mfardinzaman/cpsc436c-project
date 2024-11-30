import React, { useState, useEffect, useCallback } from "react";
import MUIDataTable from "mui-datatables";
import { TableRow, TableCell, CircularProgress, Box } from "@mui/material";

import service from "../services/services";
import { convertUnixTimeToPST } from "../utils/time";
import moment from 'moment-timezone'
import DelayLineChart from "./charts/DelayLineChart";
import VeryLateLineChart from "./charts/VeryLateLineChart";
import VeryEarlyLineChart from './charts/VeryEarlyLineChart';
import VehicleCountLineChart from "./charts/VehicleCountLineChart";

const StopsExpandableRow = ({ rowData, stop }) => {
    const [updates, setUpdates] = useState([]);
    const [updatesLoading, setUpdatesLoading] = useState(false);
    const [historicalDataLoading, setHistoricalDataLoading] = useState(false);
    const [historicalData, setHistoricalData] = useState([])

    const colSpan = rowData.length + 1;


    const getHistoricalData = useCallback(async () => {
        setHistoricalDataLoading(true)
        try {
            const result = await service.getStopStatsOverTime({
                stop_id: stop['stop_id']
            });

            if (result.statusCode === 200) {
                setHistoricalData(result.body.map((data) => ({
                    update_time: moment.utc(data.update_time).valueOf(),
                    average_delay: data.average_delay / 60,
                    early_percentage: (data.very_early_count / data.stop_count) * 100,
                    late_percentage: (data.very_late_count / data.stop_count) * 100,
                    vehicle_count: data.stop_count
                })))
            } else {
                console.log('Error fetching route stats over time:', result);
            }
        } catch (error) {
            console.log('Error:', error);
        } finally {
            setHistoricalDataLoading(false);
        }
    }, [stop])

    const getStopUpdates = useCallback(async () => {
        setUpdatesLoading(true)
        try {
            const result = await service.getStopUpdates({
                stop_id: stop['stop_id'],
            });

            if (result.statusCode === 200) {
                setUpdates(result.body)
            } else {
                console.log('Error fetching stop updates:', result);
            }
        } catch (error) {
            console.log('Error:', error);
        } finally {
            setUpdatesLoading(false);
        }
    }, [stop])

    useEffect(() => {
        getHistoricalData();
    }, [getHistoricalData])

    useEffect(() => {
        getStopUpdates();
    }, [getStopUpdates])

    return (
        <TableRow
            sx={{
                padding: 2,
            }}
        >
            <TableCell colSpan={colSpan}>
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
                {
                    updatesLoading ? <CircularProgress /> : (
                        <MUIDataTable
                            title={`Updates at Stop: ${stop.stop_name}`}
                            data={updates.map((update) => ({
                                label: update.vehicle_label,
                                number: update.route_short_name,
                                direction: update.direction_name,
                                lateness: update.delay / 60,
                                stopTime: convertUnixTimeToPST(moment.utc(update.stop_time).valueOf()),
                                lastUpdate: convertUnixTimeToPST(moment.utc(update.update_time).valueOf()),
                            }))}
                            columns={[
                                { name: "label", label: "Vehicle Label" },
                                { name: "number", label: "Number" },
                                { name: "direction", label: "Direction" },
                                {
                                    name: "lateness",
                                    label: "Delay (min)",
                                    options: {
                                        customBodyRender: (value) => Math.round(value)
                                    }
                                },
                                { name: "stopTime", label: "Stop Time" },
                                { name: "lastUpdate", label: "Last Update" },
                            ]}
                            options={{
                                selectableRows: "none",
                                pagination: false,
                                search: false,
                                print: false,
                                download: false,
                                filter: false,
                                responsive: 'standard',
                                sortOrder: {
                                    name: 'stopTime',
                                    direction: 'asc'
                                }
                            }}
                        />
                    )
                }
            </TableCell>
        </TableRow>
    );
};

export default StopsExpandableRow;
