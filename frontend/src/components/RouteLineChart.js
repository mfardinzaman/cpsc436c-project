import { CircularProgress, Box, Select, MenuItem, FormControl, InputLabel } from '@mui/material';
import React, { useEffect, useState, useCallback } from 'react'
import { LineChart, Line, CartesianGrid, XAxis, YAxis, Tooltip } from 'recharts'
import service from '../services/services'
import moment from 'moment-timezone'
import { convertUnixTimeToPST, generateTicks, filterByTimeRange } from '../utils/time'

const RouteLineChart = ({ routeId, directionId }) => {
    const [delayData, setDelayData] = useState([]);
    const [filteredData, setFilteredData] = useState([]);
    const [loading, setLoading] = useState(false);
    const [timeRange, setTimeRange] = useState('day');
    const [ticks, setTicks] = useState([]);

    const getDelayData = useCallback(async () => {
        setLoading(true)
        try {
            const result = await service.getRouteStatsOverTime({
                route_id: routeId,
                directionId
            });

            if (result.statusCode === 200) {
                setDelayData(result.body.map((data) => ({
                    update_time: moment.utc(data.update_time).valueOf(),
                    average_delay: data.average_delay / 60 // in minutes
                })))
            } else {
                console.log('Error fetching route stats over time:', result);
            }
        } catch (error) {
            console.log('Error:', error);
        } finally {
            setLoading(false);
        }
    }, [routeId, directionId])

    const tooltipLabelFormatter = (label) => convertUnixTimeToPST(label)

    const tooltipFormatter = (value) => [`${Math.round(value)} min`, 'Average delay']


    const handleTimeRangeChange = (event) => {
        const selectedRange = event.target.value;
        setTimeRange(selectedRange);
    };

    useEffect(() => {
        getDelayData();
    }, [getDelayData])

    useEffect(() => {
        setFilteredData(filterByTimeRange(delayData, timeRange));
        setTicks(generateTicks(timeRange));
    }, [setFilteredData, timeRange, delayData]);


    return (
        <Box sx={{ padding: '2%', display: 'flex', flexDirection: { xs: 'column', sm: 'row' } }}>
            <FormControl sx={{ minWidth: 200, marginBottom: 2 }}>
                <InputLabel id="time-range-label" shrink>Time Range</InputLabel>
                <Select
                    labelId="time-range-label"
                    id="time-range"
                    value={timeRange}
                    onChange={handleTimeRangeChange}
                    label="Time Range"
                    sx={{ maxWidth: { xs: '50vw', sm: 200 } }}
                >
                    <MenuItem value="day">Past Day</MenuItem>
                    <MenuItem value="week">Past Week</MenuItem>
                </Select>
            </FormControl>
            {loading ? <CircularProgress /> : (
                <Box sx={{ marginTop: '2%', padding: '2%', display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                    <LineChart width={730} height={250} data={filteredData}
                        margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis
                            dataKey="update_time"
                            domain={['auto', 'auto']}
                            name='Time'
                            label={{ value: 'Time', position: 'insideBottom', offset: -5 }}
                            ticks={ticks}
                            tickFormatter={(unixTime) => convertUnixTimeToPST(unixTime)}
                            type='number'
                        />
                        <YAxis
                            name='Average Delay'
                            label={{ value: 'Average Delay (min)', angle: -90, position: 'center', offset: 10 }}
                        />
                        <Tooltip
                            formatter={tooltipFormatter}
                            labelFormatter={tooltipLabelFormatter}
                        />
                        <Line
                            type="monotone"
                            dataKey="average_delay"
                            stroke="#8884d8"
                            dot={timeRange === 'day'}
                        />
                    </LineChart>
                </Box>
            )}

        </Box>
    )
}

export default RouteLineChart;