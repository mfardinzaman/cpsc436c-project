import React, { useEffect, useState, useCallback } from "react";
import MUIDataTable from "mui-datatables";
import { Box, Typography, TableRow, TableCell, CircularProgress } from "@mui/material";
import { Info, WarningAmber } from "@mui/icons-material";
import service from "../services/services";
import moment from 'moment-timezone'

import { convertUnixTimeToPST } from "../utils/time";
import { titleCaseToSentence } from "../utils/stringFormatter";

const WarningTitle = ({ warningAlerts }) => (
    <Box
        sx={{
            display: "flex",
            alignItems: "center",
            marginRight: 3,
        }}
    >
        <WarningAmber color="warning" />
        <Typography variant="body1" sx={{ marginLeft: 1 }}>
            {warningAlerts.length} Warnings
        </Typography>
    </Box>
)

const InfoTitle = ({ infoAlerts }) => (
    <Box
        sx={{
            display: "flex",
            alignItems: "center",
            marginRight: 3,
        }}
    >
        <Info color="info" />
        <Typography variant="body1" sx={{ marginLeft: 1 }}>
            {infoAlerts.length} Infos
        </Typography>
    </Box>
)

const AlertsSummary = () => {
    const [alerts, setAlerts] = useState([])
    const [loading, setLoading] = useState(false)

    const warningAlerts = alerts.filter((alert) => alert.severity_level === 'WARNING')
    const infoAlerts = alerts.filter((alert) => alert.severity_level === 'INFO')

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
            name: "cause",
            label: "Cause",
            options: {
                customBodyRender: (value) => titleCaseToSentence(value),
                sort: false
            }
        },
        {
            name: "effect",
            label: "Effect",
            options: {
                customBodyRender: (value) => titleCaseToSentence(value),
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
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: '16px', alignItems: 'center' }}>
            {loading ? <CircularProgress /> : (
                <>
                    <Typography variant='h2' sx={{ textAlign: 'left' }}>
                        {alerts.length} Alerts
                    </Typography>

                    <MUIDataTable
                        title={[<WarningTitle warningAlerts={warningAlerts}/>]}
                        data={warningAlerts.map((alert) => ({
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
                            pagination: true,
                            print: false,
                            download: false,
                            filter: true,
                            expandableRows: true,
                            expandableRowsHeader: false,
                            viewColumns: false,
                            sortOrder: {
                                name: "severity_level",
                                direction: "desc",
                            },
                            renderExpandableRow: (rowData, rowMeta) => {
                                const description = warningAlerts[rowMeta.dataIndex].description;

                                return (
                                    <TableRow>
                                        <TableCell colSpan={tableColumns.length + 1}>
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
                    <MUIDataTable
                        title={<InfoTitle infoAlerts={infoAlerts}/>}
                        data={infoAlerts.map((alert) => ({
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
                            pagination: true,
                            print: false,
                            download: false,
                            filter: true,
                            expandableRows: true,
                            expandableRowsHeader: false,
                            viewColumns: false,
                            sortOrder: {
                                name: "severity_level",
                                direction: "desc",
                            },
                            renderExpandableRow: (rowData, rowMeta) => {
                                const description = infoAlerts[rowMeta.dataIndex].description;

                                return (
                                    <TableRow>
                                        <TableCell colSpan={tableColumns.length + 1}>
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
                </>
            )}
        </Box>
    );
};

export default AlertsSummary;
