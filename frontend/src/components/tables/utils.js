import { createTheme } from "@mui/material";

const getMuiTheme = () => createTheme({
    components: {
        MuiPaper: {
            styleOverrides: {
                root: {
                    backgroundColor: "#dcdbe8",
                }
            }
        },
        MuiTableCell: {
            styleOverrides: {
                root: {
                    backgroundColor: "#dcdbe8"
                }
            }
        },
        MuiTableRow: {
            styleOverrides: {
                root: {
                    backgroundColor: "#dcdbe8",
                    borderColor: 'black'
                }
            }
        },
    }
})

export { getMuiTheme }