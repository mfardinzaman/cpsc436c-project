import React from "react";
import MUIDataTable from "mui-datatables";
import { Box, Typography, Accordion, AccordionDetails, AccordionSummary, TableRow, TableCell } from "@mui/material";
import { HelpOutline, Info, WarningAmber, Error } from "@mui/icons-material";
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

import mockAlerts from '../mock/mockAlerts.json';

const severityOrder = {
    SEVERE: 4,
    WARNING: 3,
    INFO: 2,
    UNKNOWN_SEVERITY: 1,
};

const severityConfig = {
    SEVERE: { icon: <Error color="error" />, label: "Severe" },
    WARNING: { icon: <WarningAmber color="warning" />, label: "Warning" },
    INFO: { icon: <Info color="info" />, label: "Info" },
    UNKNOWN_SEVERITY: { icon: <HelpOutline color="action" />, label: "Unknown Severity" },
};

const AlertsSummary = () => {

    const activeAlerts = mockAlerts; // TODO: filter for currently active alerts

    const groupedAlerts = activeAlerts.reduce((acc, alert) => {
        acc[alert.severity_level] = acc[alert.severity_level] || [];
        acc[alert.severity_level].push(alert);
        return acc;
    }, {});

    const severityCounts = Object.entries(groupedAlerts).map(([severity, alerts]) => ({
        severity,
        count: alerts.length,
    }));

    const totalAlerts = activeAlerts.length;

    const sortedAlerts = activeAlerts.sort(
        (a, b) =>
            severityOrder[b.severity_level] - severityOrder[a.severity_level] ||
            new Date(b.start) - new Date(a.start)
    );

    const tableColumns = [
        { name: "header", label: "Header" },
        { name: "severity_level", label: "Severity Level" },
        { name: "cause", label: "Cause" },
        { name: "effect", label: "Effect" },
        { name: "start", label: "Start Time" },
        { name: "end", label: "End Time" },
    ];

    return (
        <>
            <Accordion>
                <AccordionSummary
                    expandIcon={<ExpandMoreIcon />}
                    aria-controls="panel1-content"
                    id="panel1-header"
                >
                    <Box sx={{ display: "flex", alignItems: "center", width: "100%" }}>
                        <Typography variant="h6" sx={{ marginRight: 3 }}>
                            Total Alerts: {totalAlerts}
                        </Typography>
                        {severityCounts.map(({ severity, count }) => {
                            const config = severityConfig[severity] || severityConfig.UNKNOWN_SEVERITY;
                            return (
                                <Box
                                    key={severity}
                                    sx={{
                                        display: "flex",
                                        alignItems: "center",
                                        marginRight: 3,
                                    }}
                                >
                                    {config.icon}
                                    <Typography variant="subtitle1" sx={{ marginLeft: 1 }}>
                                        {config.label}: {count}
                                    </Typography>
                                </Box>
                            );
                        })}
                    </Box>
                </AccordionSummary>
                <AccordionDetails>
                    <MUIDataTable
                        title={"All Alerts"}
                        data={sortedAlerts.map((alert) => ({
                            header: alert.header,
                            severity_level: alert.severity_level,
                            cause: alert.cause,
                            effect: alert.effect,
                            start: new Date(alert.start).toLocaleString(),
                            end: new Date(alert.end).toLocaleString(),
                            description: alert.description
                        }))}
                        columns={tableColumns}
                        options={{
                            selectableRows: "none",
                            search: false,
                            pagination: true,
                            print: false,
                            download: false,
                            filter: true,
                            expandableRows: true,
                            expandableRowsHeader: false,
                            renderExpandableRow: (rowData, rowMeta) => {
                                const description = sortedAlerts[rowMeta.dataIndex].description;

                                return (
                                    <TableRow>
                                        <TableCell colSpan={tableColumns.length}>
                                            <Box>
                                                <Typography variant="body1">
                                                    {description}
                                                </Typography>
                                            </Box>
                                        </TableCell>
                                    </TableRow>
                                );
                            },
                        }}
                    />
                </AccordionDetails>
            </Accordion>
        </>
    );
};

export default AlertsSummary;
