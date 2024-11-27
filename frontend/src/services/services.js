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

const service = { getRoutes, getRouteStats, getRouteStatsOverTime }

export default service