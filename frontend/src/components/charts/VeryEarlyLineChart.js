import { Box, Select, MenuItem, FormControl, InputLabel, Typography } from '@mui/material';
import React, { useEffect, useState } from 'react'
import { LineChart, Line, CartesianGrid, XAxis, YAxis, Tooltip } from 'recharts'
import { convertUnixTimeToPST, generateTicks, filterByTimeRange } from '../../utils/time'

const VeryLateLineChart = ({ data }) => {
    const [filteredData, setFilteredData] = useState([]);
    const [timeRange, setTimeRange] = useState('day');
    const [ticks, setTicks] = useState([]);

    const minTime = Math.min(...filteredData.map((d) => d.update_time));
    const maxTime = Math.max(...filteredData.map((d) => d.update_time));

    const tooltipLabelFormatter = (label) => convertUnixTimeToPST(label)

    const tooltipFormatter = (value) => [`${value} %`, 'Percentage']


    const handleTimeRangeChange = (event) => {
        const selectedRange = event.target.value;
        setTimeRange(selectedRange);
    };

    useEffect(() => {
        setFilteredData(filterByTimeRange(data, timeRange));
        setTicks(generateTicks(timeRange));
    }, [setFilteredData, timeRange, data]);


    return (
        <Box flexDirection='column'>
            <Typography>Percentage of buses &gt; 5 min early</Typography>
            <Box sx={{ padding: '2%', display: 'flex', flexDirection: 'column' }}>
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
                <Box sx={{ marginTop: '2%', padding: '2%', display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                    <LineChart width={600} height={250} data={filteredData}
                        margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis
                            dataKey="update_time"
                            domain={[minTime, maxTime]}
                            name='Time'
                            label={{ value: 'Time', position: 'insideBottom', offset: -5 }}
                            ticks={ticks}
                            tickFormatter={(unixTime) => convertUnixTimeToPST(unixTime)}
                            type='number'
                        />
                        <YAxis
                            name='Percentage of buses > 5 minutes early'
                            label={{ value: 'Percentage', angle: -90, position: 'center', offset: 10 }}
                            allowDecimals={false}
                        />
                        <Tooltip
                            formatter={tooltipFormatter}
                            labelFormatter={tooltipLabelFormatter}
                        />
                        <Line
                            type="monotone"
                            dataKey="early_percentage"
                            stroke="#8884d8"
                            dot={timeRange === 'day'}
                        />
                    </LineChart>
                </Box>
            </Box>
        </Box>
    )
}

export default VeryLateLineChart;