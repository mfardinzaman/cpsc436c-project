import AWS from 'aws-sdk';
import React, { useEffect, useState } from "react";
import MUIDataTable from "mui-datatables";
import mockRoutes from "../mock/mockRoutes.json";
import { Typography, Box, CircularProgress } from "@mui/material";
import RouteExpandableRow from "./RouteExpandableRow";
import { retrieveRoutes } from '../utils/get_route_data';
import service from '../services/services';

const RouteDashboard = () => {
    const [routes, setRoutes] = useState(mockRoutes);
    const [loading, setLoading] = useState(false);

    const GetRoutes = async () => {
        setLoading(true);
        try {
            const result = await service.getRoutes();
    
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
            label: "Route Short Name",
        },
        {
            name: "route_long_name",
            label: "Route Long Name",
        },
        // {
        //     name: "numBuses",
        //     label: "Number of Buses",
        //     options: {
        //         searchable: false,
        //     }
        // },
        // {
        //     name: "avgLateness",
        //     label: "Average Lateness (min)",
        //     options: {
        //         searchable: false,
        //     }
        // },
        // {
        //     name: "latePercentage",
        //     label: "% > 5 Min Late",
        //     options: {
        //         searchable: false,
        //     }
        // },
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
