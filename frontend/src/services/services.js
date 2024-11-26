import fetchHelper from "./fetchHelper";

const getRoutes = async () => {
    const data = await fetchHelper('/routes', 'GET', {})
    return data
}

const getRouteStats = async () => {
    const data = await fetchHelper('/route_stats', 'GET', {})
    return data
}

const service = { getRoutes, getRouteStats }

export default service