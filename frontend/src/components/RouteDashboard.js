import AWS from 'aws-sdk'
import React, { useEffect, useState } from "react";
import MUIDataTable from "mui-datatables";
import mockRoutes from "../mock/mockRoutes.json";

import { Typography, Box } from "@mui/material";
import RouteExpandableRow from "./RouteExpandableRow";
import { retrieveRoutes } from '../utils/get_route_data';

const RouteDashboard = () => {
    const [routes, setRoutes] = useState(mockRoutes);

    const GetRoutes = async() => {
        try {
            const result = await retrieveRoutes();
            if (result.statusCode === 200) {
                setRoutes(result.body);
            }
        } catch (error) {
            console.log(error)
        }
    }
    

    const columns = [
        {
            name: "route_short_name",
            label: "Route Short Name",
        },
        {
            name: "route_long_name",
            label: "Route Long Name",
        },
        {
            name: "numBuses",
            label: "Number of Buses",
            options: {
                searchable: false
            }
        },
        {
            name: "avgLateness",
            label: "Average Lateness (min)",
            options: {
                searchable: false
            }
        },
        {
            name: "latePercentage",
            label: "% > 5 Min Late",
            options: {
                searchable: false
            }
        },
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
        filter: false
    };
    useEffect(() => {
        GetRoutes();
        // Why did I even bring ESLint here
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [])

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
            <MUIDataTable
                data={routes}
                columns={columns}
                options={options}
            />
        </Box>
    );
};

export default RouteDashboard;
