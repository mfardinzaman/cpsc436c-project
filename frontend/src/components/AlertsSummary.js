import React, { useEffect, useState, useCallback } from "react";
import MUIDataTable from "mui-datatables";
import { Box, Typography, Accordion, AccordionDetails, AccordionSummary, TableRow, TableCell, CircularProgress } from "@mui/material";
import { HelpOutline, Info, WarningAmber, Error } from "@mui/icons-material";
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import service from "../services/services";
import moment from 'moment-timezone'

import { convertUnixTimeToPST } from "../utils/time";

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
    const [alerts, setAlerts] = useState([])
    const [severityCounts, setSeverityCounts] = useState([])
    const [loading, setLoading] = useState(false)

    const fetchAlerts = useCallback(async () => {
        setLoading(true);
        try {
            const result = await service.getAlerts();

            if (result.statusCode === 200) {
                const today = moment.utc();
                const activeAlerts = result.body.filter((alert) => {
                    const start = moment.utc(alert.start);
                    const end = moment.utc(alert.end);
                    return today.isBetween(start, end, null, '[]');
                });
                setAlerts(activeAlerts)

                const groupedAlerts = activeAlerts.reduce((acc, alert) => {
                    acc[alert.severity_level] = acc[alert.severity_level] || [];
                    acc[alert.severity_level].push(alert);
                    return acc;
                }, {});
                const counts = Object.entries(groupedAlerts).map(([severity, alerts]) => ({
                    severity,
                    count: alerts.length,
                }));
                setSeverityCounts(counts)
            } else {
                console.log('Error fetching routes:', result.statusCode);
            }
        } catch (error) {
            console.log('Error:', error);
        } finally {
            setLoading(false);
        }
    }, []);

    const tableColumns = [
        {
            name: "header",
            label: "Header",
            options: {
                filter: false,
                sort: false
            }
        },
        {
            name: "severity_level",
            label: "Severity Level",
            options: {
                customSort: (data, colIndex, order) => {
                    return data.sort((a, b) => {
                        const severityA = severityOrder[a[colIndex]] || 0;
                        const severityB = severityOrder[b[colIndex]] || 0;
                        return (order === 'asc') ? severityA - severityB : severityB - severityA;
                    });
                },
            }
        },
        {
            name: "cause",
            label: "Cause",
            options: {
                sort: false
            }
        },
        {
            name: "effect",
            label: "Effect",
            options: {
                sort: false
            }
        },
        { 
            name: "start", 
            label: "Start Time",
            options: {
                customBodyRender: (value) => convertUnixTimeToPST(value.valueOf()),
                filter: false
            },
        },
        {
            name: "end",
            label: "End Time",
            options: {
                customBodyRender: (value) => {
                    return value.year() >= 2100
                        ? "Until Further Notice"
                        : convertUnixTimeToPST(value.valueOf());
                },
                filter: false
            },
        },
    ];

    useEffect(() => {
        fetchAlerts()
    }, [fetchAlerts])

    return (
        <>
            {loading ? <CircularProgress /> : (
                <Accordion>
                    <AccordionSummary
                        expandIcon={<ExpandMoreIcon />}
                        aria-controls="panel1-content"
                        id="panel1-header"
                    >
                        <Box sx={{ display: "flex", alignItems: "center", width: "100%" }}>
                            <Typography sx={{ marginRight: 3 }}>
                                Total Alerts: {alerts.length}
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
                            title={"Active Alerts"}
                            data={alerts.map((alert) => ({
                                header: alert.header,
                                severity_level: alert.severity_level,
                                cause: alert.cause,
                                effect: alert.effect,
                                start: moment.utc(alert.start),
                                end: moment.utc(alert.end),
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
                                sortOrder: {
                                    name: "severity_level",
                                    direction: "desc",
                                },
                                renderExpandableRow: (rowData, rowMeta) => {
                                    const description = alerts[rowMeta.dataIndex].description;

                                    return (
                                        <TableRow>
                                            <TableCell colSpan={tableColumns.length+1}>
                                                <Box>
                                                    <Typography variant="body2" textAlign='left'>
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
            )}
        </>
    );
};

export default AlertsSummary;
