import { CircularProgress, Paper, Box } from '@mui/material'
import React, { useEffect, useState } from 'react'
import { LineChart, Line, CartesianGrid, XAxis, YAxis, Tooltip, Legend } from 'recharts'
import service from '../services/services'
import moment from 'moment-timezone'
import { convertUnixTimeToPST } from '../utils/time'

const RouteLineChart = ({ routeId, directionId }) => {
    const [delayData, setDelayData] = useState([]);
    const [loading, setLoading] = useState(false);

    // TODO: allow user to choose last day / last week, etc.

    const getDelayData = async () => {
        setLoading(true)
        try {
            const result = await service.getRouteStatsOverTime({
                route_id: routeId,
                directionId
            });

            if (result.statusCode === 200) {
                setDelayData(result.body.map((data) => ({
                    update_time: moment(data.update_time).valueOf(),
                    average_delay: data.average_delay
                })))
                // setDelayData(result.body);
            } else {
                console.log('Error fetching route stats over time:', result);
            }
        } catch (error) {
            console.log('Error:', error);
        } finally {
            setLoading(false);
        }
    }

    const tooltipLabelFormatter = (label) => convertUnixTimeToPST(label)

    const tooltipFormatter = (value, name) => {
        return [`${Math.round(value / 60)} min`, 'Average delay']
    }

    useEffect(() => {
        getDelayData();
    }, []);

    useEffect(() => {
        console.log(delayData)
    }, [delayData])


    return (
        <Box>
            {loading ? <CircularProgress /> : (
                <Paper sx={{ marginTop: '2%', padding: '2%', display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                    <LineChart width={730} height={250} data={delayData}
                        margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis
                            dataKey="update_time"
                            domain={['auto', 'auto']}
                            name='Time'
                            tickFormatter={(unixTime) => convertUnixTimeToPST(unixTime)}
                            type='number'
                        />
                        <YAxis name='Average Delay' />
                        <Tooltip
                            formatter={tooltipFormatter}
                            labelFormatter={tooltipLabelFormatter}
                        />
                        <Line type="monotone" dataKey="average_delay" stroke="#8884d8" />
                    </LineChart>
                </Paper>
            )}

        </Box>
    )
}

export default RouteLineChart;