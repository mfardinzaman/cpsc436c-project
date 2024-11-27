import React, { useEffect, useState } from "react";
import MUIDataTable from "mui-datatables";
import mockRoutes from "../mock/mockRoutes.json";
import { Typography, Box, CircularProgress } from "@mui/material";
import RouteExpandableRow from "./RouteExpandableRow";
import service from '../services/services';

const RouteDashboard = () => {
    const [routes, setRoutes] = useState(mockRoutes);
    const [loading, setLoading] = useState(false);

    const GetRoutes = async () => {
        setLoading(true);
        try {
            const result = await service.getRouteStats();
          
            if (result.statusCode === 200) {
                setRoutes(result.body);
            } else {
                console.log('Error fetching routes:', result.statusCode);
            }
        } catch (error) {
            console.log('Error:', error);
        } finally {
            setLoading(false);
        }
    };


    const columns = [
        {
            name: "route_short_name",
            label: "Number",
        },
        {
            name: "route_long_name",
            label: "Name",
        },
        {
            name: "direction_name",
            label: "Direction"
        },
        {
            name: "vehicle_count",
            label: "Vehicle Count",
            options: {
                searchable: false
            }
        },
        {
            name: "average_delay",
            label: "Average Lateness (min)",
            options: {
                customBodyRender: (value, { rowIndex }) => {
                    const rowObject = routes[rowIndex];
                    const minutes = Math.round(value / 60)
                    return <Typography>{`${minutes}`}</Typography>
                },
                searchable: false
            }
        },
        {
            name: "very_late_count",
            label: "% Vehicles >5 minutes late",
            options: {
                customBodyRender: (value, { rowIndex }) => {
                    const rowObject = routes[rowIndex];
                    const percentage = Math.floor((value / rowObject["vehicle_count"]) * 100)
                    return <Typography>{`${percentage}%`}</Typography>
                },
                searchable: false
            }
        }
    ];

    const options = {
        selectableRows: "none",
        expandableRows: true,
        expandableRowsHeader: false,
        renderExpandableRow: (rowData, rowMeta) => (
            <RouteExpandableRow
                rowData={rowData}
                rowMeta={rowMeta}
                routes={routes}
            />
        ),
        search: true,
        print: false,
        download: false,
        filter: false,
        responsive: 'standard'
    };

    useEffect(() => {
        GetRoutes();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return (
        <Box
            sx={{
                margin: "20px auto",
                padding: 2,
            }}
        >
            <Typography variant="h4">
                Routes
            </Typography>

            {loading ? (  // Show loading indicator while fetching
                <CircularProgress />
            ) : (
                <MUIDataTable
                    data={routes}
                    columns={columns}
                    options={options}
                />
            )}
        </Box>
    );
};

export default RouteDashboard;
