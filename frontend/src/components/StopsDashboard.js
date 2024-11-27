import React, { useState } from "react";
import MUIDataTable from "mui-datatables";
import mockStops from "../mock/mockStops.json"; // Use your mock Stop_Statistic data
import mockBuses from "../mock/mockVehicles.json"; // Use your mock buses data
import { Typography, Box } from "@mui/material";
import StopExpandableRow from "./StopsExpandableRow";

const StopsDashboard = () => {
    const [stops] = useState(mockStops);

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
                searchable: false,
            },
        },
        {
            name: "high_delay_count",
            label: "Count of >5 Min Delays",
            options: {
                searchable: false,
            },
        },
        {
            name: "stop_count",
            label: "Number of Stops",
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
                rowMeta={rowMeta}
                stops={stops}
                buses={mockBuses}
            />
        ),
        search: true,
        print: false,
        download: false,
        filter: false,
        responsive: 'standard'
    };

    return (
        <Box
            sx={{
                margin: "20px auto",
                padding: 2,
            }}
        >
            <Typography variant="h4" gutterBottom>
                Stops
            </Typography>
            <MUIDataTable
                data={stops}
                columns={columns}
                options={options}
            />
        </Box>
    );
};

export default StopsDashboard;
