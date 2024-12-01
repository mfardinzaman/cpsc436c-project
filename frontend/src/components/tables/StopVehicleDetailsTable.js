import MUIDataTable from "mui-datatables";
import { convertUnixTimeToPST } from "../../utils/time";
import moment from 'moment-timezone';
import { ThemeProvider } from "@mui/material";
import { getMuiTheme } from "./utils";

const StopVehicleDetailsTable = ({ stop, updates }) => {
    return (
        <ThemeProvider theme={getMuiTheme()}>
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
                    viewColumns: false,
                    sortOrder: {
                        name: 'stopTime',
                        direction: 'asc'
                    }
                }}
            />
        </ThemeProvider>
    )
}

export default StopVehicleDetailsTable