import React, { useState } from "react";
import MUIDataTable from "mui-datatables";
import { TableRow, TableCell } from "@mui/material";

import mockBuses from "../mock/mockVehicles.json";

const RouteExpandableRow = ({ rowData, rowMeta, routes }) => {
    const [buses, setBuses] = useState(mockBuses)
    const colSpan = rowData.length + 1;

    return (
        <TableRow
            sx={{
                padding: 2,
            }}
        >
            <TableCell colSpan={colSpan}>
                <MUIDataTable
                    title={"Buses Details"}
                    data={buses.map((bus) => ({
                        label: bus.vehicle_label,
                        status: bus.current_status,
                        lateness: Math.floor(Math.random() * 15), // mock
                        lastStop: `Stop ${Math.floor(Math.random() * 10)}`, // mock
                        lastUpdate: new Date(bus.update_time).toLocaleString(),
                    }))}
                    columns={[
                        { name: "label", label: "Bus Label" },
                        { name: "status", label: "Status" },
                        { name: "lateness", label: "Lateness (min)" },
                        { name: "lastStop", label: "Last Stop" },
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
            </TableCell>
        </TableRow>
    );
};

export default RouteExpandableRow