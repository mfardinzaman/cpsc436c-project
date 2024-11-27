import moment from 'moment-timezone'

export const convertUnixTimeToPST = (unixTime) => {
    const time = moment(unixTime)
    return time.tz('America/Los_Angeles').format('YYYY-MM-DD HH:mm')
}