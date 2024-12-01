import MUIDataTable from "mui-datatables";
import { convertUnixTimeToPST } from "../../utils/time";
import moment from 'moment-timezone';
import { ThemeProvider } from "@mui/material";
import { getMuiTheme } from "./utils";

const RouteVehicleDetailsTable = ({ vehicles }) => {

    return (
        <ThemeProvider theme={getMuiTheme()}>
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
                    responsive: 'standard',
                    viewColumns: false
                }}
            />
        </ThemeProvider>
    )
}

export default RouteVehicleDetailsTable