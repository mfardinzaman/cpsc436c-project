import moment from 'moment-timezone'

export const convertUnixTimeToPST = (unixTime) => {
    const time = moment(unixTime)
    return time.tz('America/Los_Angeles').format('YYYY-MM-DD HH:mm')
}

export const generateTicks = (range) => {
    const now = moment();
    const rangeStart = range === 'day' ? now.clone().subtract(1, 'day') : now.clone().subtract(1, 'week');
    const tickInterval = range === 'day' ? 2 * 60 * 60 * 1000 : 24 * 60 * 60 * 1000;
    const roundedStart = rangeStart.clone().startOf('hour').valueOf();

    let ticks = [];
    for (let time = roundedStart; time <= now.valueOf(); time += tickInterval) {
        ticks.push(time);
    }
    return ticks
}

export const filterByTimeRange = (data, range) => {
    const now = moment();
    const rangeStart = range === 'day' ? now.clone().subtract(1, 'day') : now.clone().subtract(1, 'week');
    return data.filter((item) => moment(item.update_time).isBetween(rangeStart, now));
}