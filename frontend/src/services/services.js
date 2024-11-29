import fetchHelper from "./fetchHelper";

const getRoutes = async () => {
    const data = await fetchHelper('/routes', 'GET', {})
    return data
}

const getRouteStats = async () => {
    const data = await fetchHelper('/route_stats', 'GET', {})
    return data
}

const getRouteStatsOverTime = async ({ route_id, directionId }) => {
    const data = await fetchHelper(`/route_stats_over_time?route_id=${route_id}&direction_id=${directionId}`, 'GET',)
    return data
}

const getRouteVehicles = async ({ route_id, direction_id }) => {
    const data = await fetchHelper(`/route_vehicles?route_id=${route_id}&direction_id=${direction_id}`, 'GET',)
    return data
}

const getStopStats = async () => {
    const data = await fetchHelper('/stop_stats', 'GET', {})
    return data
}

const getStopUpdates = async ({ stop_id }) => {
    const data = await fetchHelper(`/stop_updates?stop_id=${stop_id}`, 'GET', {})
    return data
}

const getAlerts = async () => {
    const data = await fetchHelper('/alerts', 'GET', {})
    return data
}

const service = { getRoutes, getRouteStats, getRouteStatsOverTime, getRouteVehicles, getStopStats, getStopUpdates, getAlerts }

export default service