import React, { useEffect, useState, useCallback } from "react";
import MUIDataTable from "mui-datatables";
import mockStops from "../mock/mockStops.json";
import { Typography, Box, CircularProgress } from "@mui/material";
import StopExpandableRow from "./StopsExpandableRow";
import service from "../services/services";

const StopsDashboard = () => {
    const [stops, setStops] = useState(mockStops);
    const [loading, setLoading] = useState(false);

    const fetchStops = useCallback(async () => {
        setLoading(true);
        try {
            const result = await service.getStopStats();

            if (result.statusCode === 200) {
                setStops(result.body);
            } else {
                console.log('Error fetching routes:', result.statusCode);
            }
        } catch (error) {
            console.log('Error:', error);
        } finally {
            setLoading(false);
        }
    }, [])

    const columns = [
        {
            name: "stop_name",
            label: "Stop Name",
        },
        {
            name: "stop_code",
            label: "Stop Code",
        },
        {
            name: "average_delay",
            label: "Average Delay (min)",
            options: {
                customBodyRender: (value, { rowIndex }) => {
                    const minutes = Math.round(value / 60)
                    return <Typography>{`${minutes}`}</Typography>
                },
                searchable: false
            }
        },
        {
            name: "very_late_percentage",
            label: "% Vehicles > 5 Min Late",
            options: {
                customBodyRender: (value, { rowIndex }) => {
                    const percentage = value
                    return <Typography>{`${percentage}%`}</Typography>
                },
                searchable: false
            }
        },
        {
            name: "stop_count",
            label: "Number of Vehicles",
            options: {
                searchable: false,
            },
        },
    ];

    const options = {
        selectableRows: "none",
        expandableRows: true,
        expandableRowsHeader: false,
        renderExpandableRow: (rowData, rowMeta) => (
            <StopExpandableRow
                rowData={rowData}
                stop={stops[rowMeta.dataIndex]}
            />
        ),
        search: true,
        print: false,
        download: false,
        filter: false,
        responsive: 'standard',
        viewColumns: false,
        sortOrder: {
            name: 'stop_name',
            direction: 'asc'
        }
    };

    useEffect(() => {
        fetchStops()
    }, [fetchStops])

    return (
        <Box
            sx={{
                margin: "20px auto",
                padding: 2,
            }}
        >
            {
                loading ? <CircularProgress /> : (
                    <MUIDataTable
                        data={stops}
                        columns={columns}
                        options={options}
                    />
                )
            }
        </Box>
    );
};

export default StopsDashboard;
