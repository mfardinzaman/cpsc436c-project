import React, { useState, useEffect, useCallback } from "react";
import MUIDataTable from "mui-datatables";
import { TableRow, TableCell, CircularProgress, Box } from "@mui/material";

import service from "../services/services";
import { convertUnixTimeToPST } from "../utils/time";
import moment from 'moment-timezone'

const StopsExpandableRow = ({ rowData, stop }) => {
    const [updates, setUpdates] = useState([]);
    const [updatesLoading, setUpdatesLoading] = useState(false);

    const colSpan = rowData.length + 1;

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
        getStopUpdates();
    }, [getStopUpdates])

    return (
        <TableRow
            sx={{
                padding: 2,
            }}
        >
            <Box
                sx={{
                    display: 'grid',
                    gap: 2,
                    gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' },
                    padding: 2,
                }}
            >
                {/* TODO: */}
                {/* <StopDelayLineChart data={historicalData} /> */}
                {/* <StopVehicleCountLineChart data={historicalData} />
                <StopVeryLateLineChart data={historicalData} />
                <StopVeryEarlyLineChart data={historicalData} /> */}
            </Box>
            <TableCell colSpan={colSpan}>
                {
                    updatesLoading ? <CircularProgress /> : (
                        <MUIDataTable
                            title={`Updates at Stop: ${stop.stop_name}`}
                            data={updates.map((update) => ({
                                label: update.vehicle_label,
                                lateness: update.delay / 60,
                                stopTime: convertUnixTimeToPST(moment.utc(update.stop_time).valueOf()),
                                lastUpdate: convertUnixTimeToPST(moment.utc(update.update_time).valueOf()),
                            }))}
                            columns={[
                                { name: "label", label: "Vehicle Label" },
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
